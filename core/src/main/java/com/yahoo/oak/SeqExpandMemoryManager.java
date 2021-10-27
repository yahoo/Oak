/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * A Sequential-Expanding Memory Manager doesn't impose any memory manager related actions upon any
 * access. A Sequential-Expanding reference is composed of 3 parameters:
 *
 * block ID (first), offset (second) and length (third)
 *
 * All these parameters may be squashed together into one long for easy representation.
 * Using different number of bits for each parameter may incur different limitations on their sizes.
 *
 * Number of bits used for BlockID + offset gives the size of the memory that can be referenced
 * with SeqExpandMemoryManager.
 *
 * If block size is 256MB = 2^28 --> takes 28 bits
 * Then BlockID+offset have 36 bits for their representation.
 * Total memory 64GB
 *
 *
 * The Sequential-Expanding reference codec encodes the reference of the Sequential-Expanding slices
 * into a single long primitive (64 bit).
 * For example, for the block size 256MB, we need 28 bits to encode the offset
 * and additional 28 bits to encode the length.
 * So, the remaining 8 bits can encode the block id, which will limit the maximal number of blocks to 256.
 * Thus, the key/value reference encoding when using this block size (256MB) will be as follows:
 *
 *    LSB                                       MSB
 *     |     offset     |     length     | block |
 *     |     28 bit     |     28 bit     | 8 bit |
 *      0             27 28            55 56   63
 *
 * From that, we can derive that the maximal number of 1K items that can be allocated is ~128 million (2^26).
 * Note: these limitations will change for different block sizes. */
class SeqExpandMemoryManager implements MemoryManager, KeyMemoryManager  {
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
    private final ReferenceCodec rc = new ReferenceCodec(
        ReferenceCodec.AUTO_CALCULATE_BIT_SIZE, // bits# to represent block id are calculated upon other parameters
        ReferenceCodec.requiredBits(BlocksPool.getInstance().blockSize()),   // bits# to represent offset
        ReferenceCodec.requiredBits(BlocksPool.getInstance().blockSize()));  // bits# to represent length

    SeqExpandMemoryManager(BlockMemoryAllocator memoryAllocator) {
        assert memoryAllocator != null;
        this.allocator = memoryAllocator;
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
     * Present the reference as it needs to be when the target is deleted.
     * As no deletions are assumed in Sequential Expandable MM return the same reference.
     *
     * @param reference to alter
     * @return the encoded reference
     */
    @Override
    public long alterReferenceForDelete(long reference) {
        return reference;
    }

    /**
     * Provide reference considered invalid (null) by this memory manager
     */
    @Override
    public long getInvalidReference() {
        return ReferenceCodec.INVALID_REFERENCE;
    }

    @Override
    public boolean isReferenceValid(long reference) {
        return rc.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceDeleted(long reference) {
        return false;
    }

    @Override
    public boolean isReferenceValidAndNotDeleted(long reference) {
        return rc.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceConsistent(long reference) {
        return true;
    }

    @Override
    public SliceSeqExpand getEmptySlice() {
        return new SliceSeqExpand();
    }

    @Override
    public BlockMemoryAllocator getBlockMemoryAllocator() {
        return this.allocator;
    }

    @Override
    public int getHeaderSize() {
        return 0;
    }

    @Override
    public void clear(boolean clearAllocator) {
        if (clearAllocator) {
            allocator.clear();
        }
    }

    /*===================================================================*/
    /*           SliceSeqExpand                   */
    /* Inner Class for easier access to SeqExpandMemoryManager abilities */
    /*===================================================================*/

    class SliceSeqExpand extends BlockAllocationSlice implements Slice {

        /* ------------------------------------------------------------------------------------
         * Constructors
         * ------------------------------------------------------------------------------------*/
        // Should be used only by Memory Manager (within Memory Manager package)
        SliceSeqExpand() {
            super();
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
            associated = true;
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
            if (getAllocatedBlockID() == rc.getFirst(reference)) {
                // it shows performance improvement (10%) in stream scans, when only offset of the
                // key's slice is updated upon reference decoding.
                // Slice is not invalidated between next iterator steps and all the rest information
                // in slice remains the same.
                updateOnSameBlock(rc.getSecond(reference)/*offset*/, rc.getThird(reference)/*length*/);
                return true;
            }
            if (decode(reference)) {
                allocator.readMemoryAddress(this);
                return true;
            }
            return false;
        }

        /**
         * @param reference the reference to decode
         *                  (and to put the information from reference to this slice)
         * @return true if the allocation reference is valid
         */
        private boolean decode(final long reference) {
            if (!rc.isReferenceValid(reference)) {
                invalidate();
                return false;
            }

            this.blockID  = rc.getFirst(reference);
            this.offset = rc.getSecond(reference);
            this.length  = rc.getThird(reference);
            this.reference = reference;
            assert length != UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;

            // This is not the full setting of the association, therefore 'associated' flag remains false
            associated   = false;

            return true;
        }

        /**
         * Encode (create) the reference according to the information in this Slice
         *
         * @return the encoded reference
         */
        private long encodeReference() {
            return rc.encode(getAllocatedBlockID(), getAllocatedOffset(), getAllocatedLength());
        }

        // Used to duplicate the allocation state (Slice object).
        // Does not duplicate the underlying memory buffer itself.
        // Should be used when ThreadContext's internal Slice needs to be exported to the user.
        public SliceSeqExpand duplicate() {
            SliceSeqExpand newSlice = new SliceSeqExpand();
            newSlice.copyFrom(this);
            return newSlice;
        }

        /* ------------------------------------------------------------------------------------
         * Allocation info and metadata setters
         * ------------------------------------------------------------------------------------*/
        // Reset all common not final fields to invalid state
        public void invalidate() {
            blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
            reference   = ReferenceCodec.INVALID_REFERENCE;
            length      = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            offset      = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            memAddress  = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            associated  = false;
        }

        // Copy the block allocation information from another block allocation.
        public void copyFrom(Slice other) {
            copyAllocationInfoFrom((SliceSeqExpand) other);
            // if SliceSeqExpand gets new members (not included in allocation info)
            // their copy needs to be added here
        }

        /*
         * Needed only for testing!
         */
        @VisibleForTesting
        protected void associateMMAllocation(int arg1, long arg2) {
            this.reference = arg2;
        }

        // zero the underlying memory (not the header) before entering the free list
        @Override
        protected void zeroMetadata() {
            assert associated;
            assert memAddress != UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS && memAddress != 0;
            assert length != UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            UnsafeUtils.setMemory(getMetadataAddress(),
                getAllocatedLength(), // no metadata for sequentially expendable memory manager
                (byte) 0); // zero block's memory
        }

        // used only in case of iterations when the rest of the slice's data should remain the same
        // in this case once the offset is set the the slice is associated
        private void updateOnSameBlock(int offset, int length) {
            this.offset = offset;
            this.length = length;
            assert memAddress != UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
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
            return getMetadataAddress();
        }

        @Override
        public String toString() {
            return String.format(
                "SeqExpandMemoryManager.SliceSeqExpand(isAssosiated? " + associated
                    + " blockID=%d, offset=%,d, length=%,d)",
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
    
    @Override
    public <K> int compareKeyAndSerializedKey(K key, OakScopedReadBuffer serializedKey, OakComparator<K> cmp) {
        return cmp.compareKeyAndSerializedKey(key, serializedKey);
    }
    
    @Override
    public <K> int compareSerializedKeys(OakScopedReadBuffer serializedKey1,
            OakScopedReadBuffer serializedKey2, OakComparator<K> cmp) {
        return cmp.compareSerializedKeys(serializedKey1, serializedKey2);
    }
    
}
