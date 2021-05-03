/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


class SeqExpandMemoryManager implements MemoryManager {
    private final BlockMemoryAllocator allocator;

    /*
     * The direct reference codec encodes the reference of the slices (which are not subject to
     * memory reclamation) into a single long primitive (64 bit).
     * For the default block size (256MB), we need 28 bits to encode the offset
     * and additional 28 bits to encode the length.
     * So, the remaining 8 bits can encode the block id, which will limit the maximal number of blocks to 256.
     * Thus, the reference encoding when using the default block size (256MB) will be as follows:
     *
     *    LSB                                       MSB
     *     |     offset     |     length     | block |
     *     |     28 bit     |     28 bit     | 8 bit |
     *      0             27 28            55 56   63
     *
     * From that, we can derive that the maximal number of 1K items that can be allocated is ~128 million (2^26).
     * Note: these limitations will change for different block sizes.
     *
     */
    private final ReferenceCodecSeqExpand rcse;

    SeqExpandMemoryManager(BlockMemoryAllocator memoryAllocator) {
        assert memoryAllocator != null;
        this.allocator = memoryAllocator;
        rcse = new ReferenceCodecSeqExpand(
            BlocksPool.getInstance().blockSize(), BlocksPool.getInstance().blockSize(), memoryAllocator);
    }

    public void close() {
        allocator.close();
    }

    public long allocated() {
        return allocator.allocated();
    }

    public boolean isClosed() {
        return allocator.isClosed();
    }

    /**
     * Present the reference as it needs to be when the target is deleted
     *
     * @param reference to alter
     * @return the encoded reference
     */
    @Override
    public long alterReferenceForDelete(long reference) {
        return rcse.alterForDelete(reference);
    }

    /**
     * Provide reference considered invalid (null) by this memory manager
     */
    @Override
    public long getInvalidReference() {
        return ReferenceCodecSeqExpand.INVALID_REFERENCE;
    }

    @Override
    public boolean isReferenceValid(long reference) {
        return rcse.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceDeleted(long reference) {
        return rcse.isReferenceDeleted(reference);
    }

    @Override
    public boolean isReferenceValidAndNotDeleted(long reference) {
        return isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceConsistent(long reference) {
        return rcse.isReferenceConsistent(reference);
    }

    @Override
    public SliceSeqExpand getEmptySlice() {
        return new SeqExpandMemoryManager.SliceSeqExpand();
    }

    @Override
    public int getHeaderSize() {
        return 0;
    }

    /*===================================================================*/
    /*           SeqExpandMemoryManager.SliceSeqExpand                   */
    /* Inner Class for easier access to SeqExpandMemoryManager abilities */
    /*===================================================================*/

    class SliceSeqExpand extends AbstractSlice implements Slice {
        static final int UNDEFINED_LENGTH_OR_OFFSET = -1;

        /* ------------------------------------------------------------------------------------
         * Constructors
         * ------------------------------------------------------------------------------------*/
        // Should be used only by Memory Manager (within Memory Manager package)
        SliceSeqExpand() {
            super();
            invalidate();
        }

        /**
         * Allocate new off-heap cut and associated this slice with a new off-heap cut of memory
         *
         * @param size     the number of bytes required by the user
         * @param existing whether the allocation is for existing off-heap cut moving to the other
         */
        @Override
        public void allocate(int size, boolean existing) {
            boolean allocated = allocator.allocate(this, size);
            assert allocated;
            reference = encodeReference();
        }

        /**
         * When returning an allocated Slice to the Memory Manager, depending on the implementation, there might be a
         * restriction on whether this allocation is reachable by other threads or not.
         *
         * Release the associated off-heap cut, which is disconnected from the data structure,
         * but can be still accessed via threads previously having the access. It is the memory
         * manager responsibility to care for the old concurrent accesses.
         *
         * IMPORTANT NOTE:
         * It is assumed that this function is called only when the given Slice is not needed and cannot
         * be reached by any other thread. This Memory Manager doesn't provide the check for other
         * threads reachability as GC does. Therefore the Slice is moving straight to the free list of allocator.
         */
        @Override
        public void release() {
            allocator.free(this);
        }

        /**
         * Decode information from reference to this Slice's fields.
         *
         * @param reference the reference to decode
         * @return true if the given allocation reference is valid and not deleted. If reference is
         * invalid, the slice is invalidated. If reference is deleted, this slice is updated anyway.
         */
        @Override
        public boolean decodeReference(long reference) {
            if (getAllocatedBlockID() == rcse.getFirst(reference)) {
                // it shows performance improvement (10%) in stream scans, when only offset of the
                // key's slice is updated upon reference decoding.
                // Slice is not invalidated between next iterator steps and all the rest information
                // in slice remains the same.
                updateOnSameBlock(rcse.getSecond(reference)/*offset*/, rcse.getThird(reference)/*length*/);
                return true;
            }
            if (rcse.decode(this, reference)) {
                allocator.readMemoryAddress(this);
                return true;
            }
            return false;
        }

        /**
         * Encode (create) the reference according to the information in this Slice
         *
         * @return the encoded reference
         */
        @Override
        public long encodeReference() {
            return rcse.encode(getAllocatedBlockID(), getAllocatedOffset(), getAllocatedLength());
        }

        // Used to duplicate the allocation state (Slice object).
        // Does not duplicate the underlying memory buffer itself.
        // Should be used when ThreadContext's internal Slice needs to be exported to the user.
        public SeqExpandMemoryManager.SliceSeqExpand getDuplicatedSlice() {
            SeqExpandMemoryManager.SliceSeqExpand newSlice =
                new SeqExpandMemoryManager.SliceSeqExpand();
            newSlice.copyFrom(this);
            return newSlice;
        }

        /* ------------------------------------------------------------------------------------
         * Allocation info and metadata setters
         * ------------------------------------------------------------------------------------*/
        // Reset all common not final fields to invalid state
        public void invalidate() {
            blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
            reference   = ReferenceCodecSeqExpand.INVALID_REFERENCE;
            length      = undefinedLengthOrOffsetOrAddress;
            offset      = undefinedLengthOrOffsetOrAddress;
            memAddress  = undefinedLengthOrOffsetOrAddress;
            associated  = false;
        }

        /*
         * Updates everything that can be extracted with reference decoding of "SeqExpand" type,
         * including reference itself.
         * This is not the full setting of the association, therefore 'associated' flag remains false
         */
        @Override
        void associateReferenceDecoding(int blockID, int offset, int length, long reference) {
            // length can remain undefined until requested, but if given length should include the header
            assert length != UNDEFINED_LENGTH_OR_OFFSET;
            setBlockIdOffsetAndLength(blockID, offset, length);
            this.reference = reference;
            associated   = false;
        }

        // Copy the block allocation information from another block allocation.
        public <T extends Slice> void copyFrom(T other) {
            if (other instanceof SeqExpandMemoryManager.SliceSeqExpand) {
                //TODO: any other ideas instead of `instanceof` STILL?
                // Maybe can be resolve with introduction of the Slice interface...
                copyAllocationInfoFrom((SeqExpandMemoryManager.SliceSeqExpand) other);
                // if SeqExpandMemoryManager.SliceSeqExpand gets new members (not included in allocation info)
                // their copy needs to be added here
            } else {
                throw new IllegalStateException(
                    "Must provide SeqExpandMemoryManager.SliceSeqExpand other Slice");
            }
        }

        /*
         * Needed only for testing!
         */
        @VisibleForTesting
        void associateMMAllocation(int arg1, long arg2) {
            this.reference = arg2;
        }

        // used only in case of iterations when the rest of the slice's data should remain the same
        // in this case once the offset is set the the slice is associated
        private void updateOnSameBlock(int offset, int length) {
            this.offset = offset;
            this.length = length;
            assert memAddress != undefinedLengthOrOffsetOrAddress;
            this.associated = true;
        }

        /* ------------------------------------------------------------------------------------
         * Allocation info getters
         * ------------------------------------------------------------------------------------*/
        public int getAllocatedLength() {
            assert associated;
            return length;
        }

        /* ------------------------------------------------------------------------------------
         * Metadata getters
         * ------------------------------------------------------------------------------------*/
        @Override
        public int getLength() {
            return length;
        }

        @Override
        public long getAddress() {
            return memAddress + offset;
        }

        @Override
        public String toString() {
            return String.format(
                "SeqExpandMemoryManager.SliceSeqExpand(blockID=%d, offset=%,d, length=%,d)",
                blockID, offset, length);
        }

        /*-------------- Off-heap header operations: locking and logical delete --------------*/

        /**
         * Acquires a read lock.
         * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
         *
         * @return {@code TRUE} if the read lock was acquires successfully
         * {@code FALSE} if the header/off-heap-cut is marked as deleted
         * {@code RETRY} if the header/off-heap-cut was moved, or the version of the off-heap header
         * does not match {@code version}.
         */
        public ValueUtils.ValueResult lockRead() {
            return ValueUtils.ValueResult.TRUE;
        }

        /**
         * Releases a read lock.
         * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
         *
         * @return {@code TRUE} if the read lock was released successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult unlockRead() {
            return ValueUtils.ValueResult.TRUE;
        }

        /**
         * Acquires a write lock.
         * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
         *
         * @return {@code TRUE} if the write lock was acquires successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult lockWrite() {
            return ValueUtils.ValueResult.TRUE;
        }

        /**
         * Releases a write lock.
         * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
         *
         * @return {@code TRUE} if the write lock was released successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult unlockWrite() {
            return ValueUtils.ValueResult.TRUE;
        }

        /**
         * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
         * Expand-only Slice doesn't support deletion, therefore for this type of slice this is NOP.
         *
         * @return {@code TRUE} if the value was marked successfully
         * {@code FALSE} if the value is already marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult logicalDelete() {
            return ValueUtils.ValueResult.TRUE;
        }

        /**
         * Is the associated off-heap cut marked as logically deleted
         *
         * @return {@code TRUE} if the value is marked
         * {@code FALSE} if the value is not marked
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult isDeleted() {
            return ValueUtils.ValueResult.FALSE;
        }

        /**
         * Marks the header of the associated off-heap cut as moved, just write (without CAS)
         * The write lock must be held (asserted inside the header)
         *
         * Expand-only Slice doesn't support move (for now), therefore for this type of slice this is NOP.
         */
        public void markAsMoved() { }

        /**
         * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
         * The write lock must be held (asserted inside the header).
         * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
         *
         * Expand-only Slice doesn't support deletion, therefore for this type of slice this is NOP.
         */
        public void markAsDeleted() { }

    }
}

