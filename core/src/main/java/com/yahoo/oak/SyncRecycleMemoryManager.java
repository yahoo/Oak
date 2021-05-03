/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

class SyncRecycleMemoryManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    private static final SyncRecycleMMHeader HEADER =
        new SyncRecycleMMHeader(); // for off-heap header operations
    private static final int VERS_INIT_VALUE = 1;
    private static final int OFF_HEAP_HEADER_SIZE = 12; /* Bytes */
    private final ThreadIndexCalculator threadIndexCalculator;
    private final List<List<SyncRecycleMemoryManager.SliceSyncRecycle>> releaseLists;
    private final AtomicInteger globalVersionNumber;
    private final BlockMemoryAllocator allocator;

    /*
     * The VALUE_RC reference codec encodes the reference (with memory manager abilities) of the values
     * into a single long primitive (64 bit).
     * For encoding details please take a look on ReferenceCodecSyncRecycle
     *
     */
    private final ReferenceCodecSyncRecycle rcsr;

    SyncRecycleMemoryManager(BlockMemoryAllocator allocator) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalVersionNumber = new AtomicInteger(VERS_INIT_VALUE);
        this.allocator = allocator;
        rcsr = new ReferenceCodecSyncRecycle(BlocksPool.getInstance().blockSize(), allocator);
    }

    @Override
    public void close() {
        allocator.close();
    }

    @Override
    public boolean isClosed() {
        return allocator.isClosed();
    }

    // used only for testing
    @VisibleForTesting
    int getCurrentVersion() {
        return globalVersionNumber.get();
    }

    /**
     * Present the reference as it needs to be when the target is deleted
     *
     * @param reference to alter
     * @return the encoded reference
     */
    @Override
    public long alterReferenceForDelete(long reference) {
        return rcsr.alterForDelete(reference);
    }

    /**
     * Provide reference considered invalid (null) by this memory manager
     */
    @Override
    public long getInvalidReference() {
        return ReferenceCodecSyncRecycle.INVALID_REFERENCE;
    }

    @Override
    public boolean isReferenceValid(long reference) {
        return rcsr.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceDeleted(long reference) {
        return rcsr.isReferenceDeleted(reference);
    }

    @Override public boolean isReferenceValidAndNotDeleted(long reference) {
        return rcsr.isReferenceValidAndNotDeleted(reference);
    }

    @Override
    public boolean isReferenceConsistent(long reference) {
        return rcsr.isReferenceConsistent(reference);
    }

    @Override
    public SyncRecycleMemoryManager.SliceSyncRecycle getEmptySlice() {
        return new SyncRecycleMemoryManager.SliceSyncRecycle(OFF_HEAP_HEADER_SIZE, HEADER);
    }

    @VisibleForTesting
    @Override
    public int getHeaderSize() {
        return OFF_HEAP_HEADER_SIZE;
    }

    @Override
    public long allocated() {
        return allocator.allocated();
    }

    // The version takes specific number of bits (including delete bit)
    // the version increasing needs to restart once the maximal number of bits is reached
    //
    // Increasing global version can be done concurrently by number of threads.
    // In order not to increase and overwrite allowed number of bits, increase is done via
    // atomic CAS.
    //
    private void increaseGlobalVersion() {
        // the version takes specific number of bits (including delete bit)
        // version increasing needs to restart once the maximal number of bits is reached
        int curVer = globalVersionNumber.get();
        if (curVer == ReferenceCodecSyncRecycle.LAST_VALID_VERSION) {
            globalVersionNumber.compareAndSet(curVer, VERS_INIT_VALUE);
        } else {
            globalVersionNumber.compareAndSet(curVer, curVer + 1);
        }
        // if CAS fails someone else updated the version, which is good enough
    }

    /*=====================================================================*/
    /*           SyncRecycleMemoryManager.SliceSyncRecycle                 */
    /* Inner Class for easier access to SyncRecycleMemoryManager abilities */
    /*=====================================================================*/

    /**
     * SyncRecycleMemoryManager.SliceSyncRecycle represents an data about an off-heap cut:
     * a portion of a bigger block, which is part of the underlying
     * (recycling and synchronized) managed off-heap memory.
     * SyncRecycleMemoryManager.SliceSyncRecycle is allocated only via SyncRecycleMemoryManager,
     * and can be de-allocated later. Any slice can be either empty or associated with an off-heap cut,
     * which is the aforementioned portion of an off-heap memory.
     */
    class SliceSyncRecycle extends AbstractSlice {
        static final int UNDEFINED_LENGTH_OR_OFFSET = -1;

        /**
         * An allocated by SyncRecycleMemoryManager off-heap cut have reserved space for meta-data, i.e., a header.
         * The header size is defined externally by memory-manager at the slice construction.
         */
        private final int headerSize;
        private final SyncRecycleMMHeader header;
        private int version;    // Allocation time version

        /* ------------------------------------------------------------------------------------
         * Constructors
         * ------------------------------------------------------------------------------------*/
        // Should be used only by Memory Manager (within Memory Manager package)
        SliceSyncRecycle(int headerSize, SyncRecycleMMHeader header) {
            super();
            this.headerSize = headerSize;
            this.header = header;
            invalidate();
        }

        /**
         * Allocate new off-heap cut and associated this slice with a new off-heap cut of memory
         *
         * 1. Native memory manager requires metadata header to be placed before the user data written
         *    off-heap, therefore the bigger than requested size is allocated
         * 2. The parameter flag existing explains whether the allocation is for existing slice
         *    moving to the other location (e.g. in order to be enlarged). The algorithm of move requires
         *    the lock of the newly allocated slice to be taken exclusively until the process of
         *    move is finished.
         *
         * @param size     the number of bytes required by the user
         * @param existing whether the allocation is for existing off-heap cut moving to the other
         */
        @Override
        public void allocate(int size, boolean existing) {
            boolean allocated = allocator.allocate(this, size + OFF_HEAP_HEADER_SIZE);
            assert allocated;
            int allocationVersion = globalVersionNumber.get();
            version = allocationVersion;
            // Initiate the header that is serving for synchronization and memory management
            // for value written for the first time (not existing):
            //      initializing the header's lock to be free
            // for value being moved (existing): initialize the lock to be locked
            if (existing) {
                HEADER.initLockedHeader(getMetadataAddress(), size, allocationVersion);
            } else {
                HEADER.initFreeHeader(getMetadataAddress(), size, allocationVersion);
            }
            assert HEADER.getOffHeapVersion(getMetadataAddress()) == allocationVersion;
            reference = encodeReference();
        }

        /**
         * Release the associated off-heap cut, which is disconnected from the data structure,
         * but can be still accessed via threads previously having the access. It is the memory
         * manager responsibility to care for the old concurrent accesses.
         */
        @Override
        public void release() {
            prefetchDataLength(); // this will set the length from off-heap header, if needed
            int idx = threadIndexCalculator.getIndex();
            List<SyncRecycleMemoryManager.SliceSyncRecycle> myReleaseList = releaseLists.get(idx);
            // ensure the length of the slice is always set
            myReleaseList.add(getDuplicatedSlice());
            if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
                increaseGlobalVersion();
                for (SyncRecycleMemoryManager.SliceSyncRecycle allocToRelease : myReleaseList) {
                    allocator.free(allocToRelease);
                }
                myReleaseList.clear();
            }
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
            // reference is set in the slice as part of decoding
            if (SyncRecycleMemoryManager.this.rcsr.decode(this, reference)) {
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
            return rcsr.encode(getAllocatedBlockID(), getAllocatedOffset(), getVersion());
        }

        // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
        // Should be used when ThreadContext's internal Slice needs to be exported to the user.
        public SyncRecycleMemoryManager.SliceSyncRecycle getDuplicatedSlice() {
            SyncRecycleMemoryManager.SliceSyncRecycle
                newSlice = new SyncRecycleMemoryManager.SliceSyncRecycle(this.headerSize, this.header);
            newSlice.copyFrom(this);
            return newSlice;
        }

        /* ------------------------------------------------------------------------------------
         * Allocation info and metadata setters
         * ------------------------------------------------------------------------------------*/
        // Reset all not final fields to invalid state
        public void invalidate() {
            blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
            reference   = ReferenceCodecSyncRecycle.INVALID_REFERENCE;
            version     = ReferenceCodecSyncRecycle.INVALID_VERSION;
            length      = UNDEFINED_LENGTH_OR_OFFSET;
            offset      = UNDEFINED_LENGTH_OR_OFFSET;
            memAddress  = undefinedLengthOrOffsetOrAddress;
            associated  = false;
        }

        /*
         * Updates everything that can be extracted with reference decoding of "SyncRecycle" type,
         * including reference itself. The separation by reference decoding type is temporary!
         * This is not the full setting of the association, therefore 'associated' flag remains false
         */
        @Override
        void associateReferenceDecoding(int blockID, int offset, int version, long reference) {
            setBlockIdOffsetAndLength(blockID, offset, UNDEFINED_LENGTH_OR_OFFSET);
            this.reference = reference;
            this.version = version;
            associated   = false;
        }

        // Copy the block allocation information from another block allocation.
        public <T extends Slice> void copyFrom(T other) {
            if (other instanceof SyncRecycleMemoryManager.SliceSyncRecycle) { //TODO: any other ideas?
                copyAllocationInfoFrom((SyncRecycleMemoryManager.SliceSyncRecycle) other);
                this.version = ((SyncRecycleMemoryManager.SliceSyncRecycle) other).version;
                // if SeqExpandMemoryManager.SliceSeqExpand gets new members (not included in allocation info)
                // their copy needs to be added here
            } else {
                throw new IllegalStateException("Must provide SyncRecycleMemoryManager.SliceSyncRecycle other Slice");
            }
        }

        /*
         * Needed only for testing!
         */
        @VisibleForTesting
        void associateMMAllocation(int arg1, long arg2) {
            this.version = arg1;
            this.reference = arg2;
        }

        // the method has no effect if length is already set
        void prefetchDataLength() {
            if (length == UNDEFINED_LENGTH_OR_OFFSET) {
                // the length kept in header is the length of the data only!
                // add header size
                this.length = header.getDataLength(getMetadataAddress()) + headerSize;
            }
        }

        /* ------------------------------------------------------------------------------------
         * Allocation info getters
         * ------------------------------------------------------------------------------------*/
        public int getAllocatedLength() {
            assert associated;
            // prefetchDataLength() prefetches the length from header only if Slice's length is undefined
            prefetchDataLength();
            return length;
        }

        /* ------------------------------------------------------------------------------------
         * Metadata getters
         * ------------------------------------------------------------------------------------*/
        int getVersion() {
            return version;
        }

        void setVersion(int version) {
            this.version = version;
        }

        @Override
        public int getLength() {
            // prefetchDataLength() prefetches the length from header only if Slice's length is undefined
            prefetchDataLength();
            return length - headerSize;
        }

        @Override
        public long getAddress() {
            return memAddress + offset + headerSize;
        }

        @Override
        public String toString() {
            return String.format(
                "SyncRecycleMemoryManager.SliceSyncRecycle(blockID=%d, offset=%,d, length=%,d, version=%d)",
                blockID, offset, length, version);
        }

        /*-------------- Off-heap header operations: locking and logical delete --------------*/

        /**
         * Acquires a read lock
         *
         * @return {@code TRUE} if the read lock was acquires successfully
         * {@code FALSE} if the header/off-heap-cut is marked as deleted
         * {@code RETRY} if the header/off-heap-cut was moved, or the version of the off-heap header
         * does not match {@code version}.
         */
        public ValueUtils.ValueResult lockRead() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return header.lockRead(version, getMetadataAddress());
        }

        /**
         * Releases a read lock
         *
         * @return {@code TRUE} if the read lock was released successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult unlockRead() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return header.unlockRead(version, getMetadataAddress());
        }

        /**
         * Acquires a write lock
         *
         * @return {@code TRUE} if the write lock was acquires successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult lockWrite() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return header.lockWrite(version, getMetadataAddress());
        }

        /**
         * Releases a write lock
         *
         * @return {@code TRUE} if the write lock was released successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult unlockWrite() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return header.unlockWrite(version, getMetadataAddress());
        }

        /**
         * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
         *
         * @return {@code TRUE} if the value was marked successfully
         * {@code FALSE} if the value is already marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult logicalDelete() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return header.logicalDelete(version, getMetadataAddress());
        }

        /**
         * Is the associated off-heap cut marked as logically deleted
         *
         * @return {@code TRUE} if the value is marked
         * {@code FALSE} if the value is not marked
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult isDeleted() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return header.isLogicallyDeleted(version, getMetadataAddress());
        }

        /**
         * Marks the header of the associated off-heap cut as moved, just write (without CAS)
         * The write lock must be held (asserted inside the header)
         */
        public void markAsMoved() {
            assert associated;
            header.markAsMoved(getMetadataAddress());
        }

        /**
         * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
         * The write lock must be held (asserted inside the header).
         * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
         */
        public void markAsDeleted() {
            assert associated;
            header.markAsDeleted(getMetadataAddress());
        }
    }
}
