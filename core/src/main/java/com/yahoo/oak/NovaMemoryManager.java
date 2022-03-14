/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLongArray;


class NovaMemoryManager extends SyncRecycleMemoryManager {
    static final int CACHE_PADDING = 8;
    static final int INVALID_ENTRY = 0;
    static final int REFENTRY = 1;
    
    private static final NovaMMHeader HEADER =
        new NovaMMHeader(); // for off-heap header operations
    private static final int OFF_HEAP_HEADER_SIZE = 8; /* Bytes */
    private static final long DELETED_BIT_INDEX = 1;
    
    private final List<List<SliceNova>> releaseLists;   
    private final AtomicLongArray tap;

    NovaMemoryManager(BlockMemoryAllocator allocator) {
        super(allocator);
        this.tap = new AtomicLongArray(ThreadIndexCalculator.MAX_THREADS * CACHE_PADDING * 2);
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        for (int i = CACHE_PADDING; i < ThreadIndexCalculator.MAX_THREADS * CACHE_PADDING * 2; i += CACHE_PADDING) {
            this.tap.set(i + REFENTRY, INVALID_ENTRY); 
        }  
        rc = new ReferenceCodecSyncRecycle(BlocksPool.getInstance().blockSize(), allocator, DELETED_BIT_INDEX);
        
    }

    @Override
    public SliceNova getEmptySlice() {
        return new SliceNova();
    }
    
    @VisibleForTesting
    @Override
    public int getHeaderSize() {
        return OFF_HEAP_HEADER_SIZE;
    }
    
    @VisibleForTesting
    int getCurrentVersion() {
        return globalVersionNumber.get();
    }
    
    public static void setTap(AtomicLongArray tap, long ref, int idx) {
        int i = idx % ThreadIndexCalculator.MAX_THREADS;
        tap.set(CACHE_PADDING * (i + 1) + REFENTRY, ref);
    }
        
    public static void resetTap(AtomicLongArray tap, int idx) {
        int i = idx % ThreadIndexCalculator.MAX_THREADS;
        tap.set(CACHE_PADDING * (i + 1) + REFENTRY, INVALID_ENTRY);
    }

    /*=====================================================================*/
    /*           SliceNova                 */
    /* Inner Class for easier access to NovaMemoryManager abilities */
    /*=====================================================================*/

    /**
     * SliceNova represents an data about an off-heap cut:
     * a portion of a bigger block, which is part of the underlying
     * (recycling and synchronized) managed off-heap memory.
     * SliceNova is allocated only via NovaMemoryManager,
     * and can be de-allocated later. Any slice can be either empty or associated with an off-heap cut,
     * which is the aforementioned portion of an off-heap memory.
     */
    class SliceNova extends SliceSyncRecycle {

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
            associated = true;
            version = globalVersionNumber.get() << 1; //version with 0 as not deleted bit
            HEADER.initHeader(memAddress + offset, size, version);
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
            List<SliceNova> myReleaseList = releaseLists.get(idx);
            // ensure the length of the slice is always set
            myReleaseList.add(duplicate());
            if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
                ArrayList<Long> hostageSlices = new ArrayList<>();
                for (int i = CACHE_PADDING; 
                    i < 2 * CACHE_PADDING * ThreadIndexCalculator.MAX_THREADS; i = i + CACHE_PADDING ) {
                    if (tap.get(i + REFENTRY) != INVALID_ENTRY) {
                        hostageSlices.add(tap.get(i + REFENTRY));
                    }
                } //TODO remove this to discuss
                increaseGlobalVersion(); 
                //TODO to ease contention maybe increase just if Global is smaller 
                //that the smallest slice we have, or to increase Version on allocation
                SliceNova toDeleteObj;
                for (int iret = 0; iret < myReleaseList.size();) {
                    toDeleteObj = myReleaseList.get(iret);
                    long tap = rc.encode(blockID, offset, 0);
                    if (! hostageSlices.contains(tap)) {
                        myReleaseList.remove(toDeleteObj);
                        allocator.free(toDeleteObj);
                        continue;
                    }
                    iret++;
                }
            }
        }
        
        
        public void releasesUnpublished() { //currently not used all calls use the release method in 
            //order to release key, this can be used in cases where the key failed to be inserted 
            //and was not exposed to all other threads for immediate release.
            prefetchDataLength(); // this will set the length from off-heap header, if needed
            allocator.free(this);
        }
        
        @Override
        public void copyFrom(Slice other) {
            copyAllocationInfoFrom((SliceNova) other);
            this.version = ((SliceNova) other).version;
        }
        
        @Override
        void prefetchDataLength() {
            if (length == UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS) {
                // the length kept in header is the length of the data only!
                // add header size
                this.length = HEADER.getDataLength(getMetadataAddress()) + OFF_HEAP_HEADER_SIZE;
            }
        }

        @Override
        public SliceNova duplicate() {
            SliceNova newSlice = new SliceNova();
            newSlice.copyFrom(this);
            return newSlice;
        }
        
        @Override
        public int getLength() {
            // prefetchDataLength() prefetches the length from header only if Slice's length is undefined
            prefetchDataLength();
            return length - OFF_HEAP_HEADER_SIZE;
        }

        @Override
        public long getAddress() {
            return memAddress + offset + OFF_HEAP_HEADER_SIZE;
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
        public ValueUtils.ValueResult preRead() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return HEADER.preRead(version, getMetadataAddress());
        }

        /**
         * Releases a read lock
         *
         * @return {@code TRUE} if the read lock was released successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult postRead() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return HEADER.postRead(version, getMetadataAddress());
        }

        /**
         * Acquires a write lock
         *
         * @return {@code TRUE} if the write lock was acquires successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult preWrite() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return HEADER.preWrite(version, rc.encode(blockID, offset), getMetadataAddress(), tap);
        }

        /**
         * Releases a write lock
         *
         * @return {@code TRUE} if the write lock was released successfully
         * {@code FALSE} if the value is marked as deleted
         * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
         */
        public ValueUtils.ValueResult postWrite() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            return HEADER.postWrite(version, getMetadataAddress(),  tap);
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
            return HEADER.logicalDelete(version, getMetadataAddress());
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
            return HEADER.isLogicallyDeleted(version, getMetadataAddress());
        }

        /**
         * Marks the header of the associated off-heap cut as moved, just write (without CAS)
         * The write lock must be held (asserted inside the header)
         */
        public void markAsMoved() {
            assert associated;
            HEADER.markAsMoved(getMetadataAddress());
            throw new IllegalAccessError();
        }

        /**
         * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
         * The write lock must be held (asserted inside the header).
         * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
         */
        public void markAsDeleted() {
            assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
            HEADER.logicalDelete(version, getMetadataAddress());
        }

    }
}
