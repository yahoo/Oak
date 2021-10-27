/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.ValueUtils.ValueResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;


class NovaMemoryManager implements MemoryManager, KeyMemoryManager {
    static final int RELEASE_LIST_LIMIT = 124;
    static final int CACHE_PADDING = 8;
    static final int IDENTRY = 0;
    static final int REFENTRY = 1;
    
    private static final NovaMMHeader HEADER =
        new NovaMMHeader(); // for off-heap header operations
    private static final int VERS_INIT_VALUE = 1;
    private static final int OFF_HEAP_HEADER_SIZE = 8; /* Bytes */
    private static final int VERSION_SIZE = 22;  // block offset get 42 bits on heap -  (including one bit for deleted)

    
    
    private final ThreadIndexCalculator threadIndexCalculator;
    private final List<List<SliceNova>> releaseLists;
    private final AtomicInteger globalVersionNumber;
    private final BlockMemoryAllocator allocator;

    
    private final AtomicLongArray tap;

    /*
     * The VALUE_RC reference codec encodes the reference (with memory manager abilities) of the values
     * into a single long primitive (64 bit).
     * For encoding details please take a look on ReferenceCodecSyncRecycle
     *
     */
    private final ReferenceCodecNova rc;

    NovaMemoryManager(BlockMemoryAllocator allocator) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.tap = new AtomicLongArray(ThreadIndexCalculator.MAX_THREADS * CACHE_PADDING * 2);
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        for (int i = CACHE_PADDING; i < ThreadIndexCalculator.MAX_THREADS * CACHE_PADDING * 2; i += CACHE_PADDING) {
            this.tap.set(i + IDENTRY, -1); 
        }  
        globalVersionNumber = new AtomicInteger(VERS_INIT_VALUE);
        this.allocator = allocator;
        rc = new ReferenceCodecNova(BlocksPool.getInstance().blockSize(), allocator);
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
        return rc.alterForDelete(reference);
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
        return rc.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceDeleted(long reference) {
        return rc.isReferenceDeleted(reference); //rc.getThird(reference)%2 == 1 ? true : false;
    }

    @Override public boolean isReferenceValidAndNotDeleted(long reference) {
        return rc.isReferenceValidAndNotDeleted(reference);
    }

    @Override
    public boolean isReferenceConsistent(long reference) {
        return rc.isReferenceConsistent(reference);
    }

    @Override
    public SliceNova getEmptySlice() {
        return new SliceNova();
    }

    @Override
    public BlockMemoryAllocator getBlockMemoryAllocator() {
        return this.allocator;
    }
    
    @VisibleForTesting
    @Override
    public int getHeaderSize() {
        return OFF_HEAP_HEADER_SIZE;
    }
    
    @Override
    public void clear(boolean clearAllocator) { //TODO ASK the motive for this method
        if (clearAllocator) {
            allocator.clear();
        }
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalVersionNumber.set(VERS_INIT_VALUE);
    }

    @Override
    public long allocated() {
        return allocator.allocated();
    }

    
    public  void setTap(long ref, int idx) {
        int i = idx % ThreadIndexCalculator.MAX_THREADS;
        tap.set(CACHE_PADDING * (i + 1) + IDENTRY, idx);
        tap.set(CACHE_PADDING * (i + 1) + REFENTRY, ref);
    }
        
    public  void unsetTap(int idx) {
        int i = idx % ThreadIndexCalculator.MAX_THREADS;
        tap.set(CACHE_PADDING * (i + 1) + IDENTRY, -1);
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
    /*           SliceSyncRecycle                 */
    /* Inner Class for easier access to SyncRecycleMemoryManager abilities */
    /*=====================================================================*/

    /**
     * SliceSyncRecycle represents an data about an off-heap cut:
     * a portion of a bigger block, which is part of the underlying
     * (recycling and synchronized) managed off-heap memory.
     * SliceSyncRecycle is allocated only via SyncRecycleMemoryManager,
     * and can be de-allocated later. Any slice can be either empty or associated with an off-heap cut,
     * which is the aforementioned portion of an off-heap memory.
     */
    class SliceNova extends BlockAllocationSlice {

        private int version;    // Allocation time version

        /* ------------------------------------------------------------------------------------
         * Constructors
         * ------------------------------------------------------------------------------------*/
        // Should be used only by Memory Manager (within Memory Manager package)
        SliceNova() {
            super();
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
            associated = true;
            version = globalVersionNumber.get() << 1;
            HEADER.initHeader(memAddress + offset, size, version);
            this.setHeader(size);
            reference = encodeReference();
        }
        
        void setHeader(int size) {
            long headerSlice    = (long) (size) << VERSION_SIZE;
            int  newVer         = version & 0x1FFFFF; //VERSION_SIZE;
            long header         = headerSlice | newVer ;
            UnsafeUtils.UNSAFE.putLong(memAddress + offset, header);
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
                    if (tap.get(i + IDENTRY) != -1) {
                        hostageSlices.add(tap.get(i + REFENTRY));
                    }
                }
                increaseGlobalVersion(); 
                //TODO to ease contention maybe increase just if Global is smaller 
                //that the smallest slice we have, or to increase Version on allocation
                SliceNova toDeleteObj;
                for (int iret = 0; iret < myReleaseList.size();) {
                    toDeleteObj = myReleaseList.get(iret);
                    if (! hostageSlices.contains(toDeleteObj.getDelReference())) {
                        myReleaseList.remove(toDeleteObj);
                        allocator.free(toDeleteObj);
                        continue;
                    }
                    iret++;
                }
            }
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

        public long getDelReference() {
            int ref = blockID;
            return ref << 20 | offset;
        }
        
        
        public void releasefast() {
            prefetchDataLength(); // this will set the length from off-heap header, if needed
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
            // reference is set in the slice as part of decoding
            if (decode(reference)) {
                allocator.readMemoryAddress(this);
                return true;
            }
            return false;
        }

        /**
         * @param reference the reference to decode
         * @return true if the allocation reference is valid
         */
        private boolean decode(final long reference) {
            if (!rc.isReferenceValid(reference)) {
                invalidate();
                return false;
            }

            this.blockID  = rc.getFirst(reference);
            this.offset = rc.getSecond(reference);
            this.version  = rc.getThird(reference);
            this.length = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            this.reference = reference;

            // This is not the full setting of the association, therefore 'associated' flag remains false
            associated   = false;

            return !isReferenceDeleted(reference);
        }

        /**
         * Encode (create) the reference according to the information in this Slice
         *
         * @return the encoded reference
         */
        private long encodeReference() {
            return rc.encode(getAllocatedBlockID(), getAllocatedOffset(), getVersion());
        }

        // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
        // Should be used when ThreadContext's internal Slice needs to be exported to the user.
        public SliceNova duplicate() {
            SliceNova newSlice = new SliceNova();
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
            length      = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            offset      = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            memAddress  = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
            associated  = false;
        }

        // Copy the block allocation information from another block allocation.
        public void copyFrom(Slice other) {
            copyAllocationInfoFrom((SliceNova) other);
            this.version = ((SliceNova) other).version;
            // if SliceSyncRecycle gets new members (not included in allocation info)
            // their copy needs to be added here
        }

        /*
         * Needed only for testing!
         */
        @VisibleForTesting
        protected void associateMMAllocation(int arg1, long arg2) {
            this.version = arg1;
            this.reference = arg2;
        }

        // the method has no effect if length is already set
        void prefetchDataLength() {
            if (length == UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS) {
                // the length kept in header is the length of the data only!
                // add header size
                this.length = HEADER.getDataLength(getMetadataAddress()) + OFF_HEAP_HEADER_SIZE;
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
            return length - OFF_HEAP_HEADER_SIZE;
        }

        @Override
        public long getAddress() {
            return memAddress + offset + OFF_HEAP_HEADER_SIZE;
        }

        @Override
        public String toString() {
            return String.format(
                "SyncRecycleMemoryManager.SliceSyncRecycle(isAssociated? " + associated
                    + " blockID=%d, offset=%,d, length=%,d, version=%d)",
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
            return HEADER.lockRead(version, getMetadataAddress());
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
            return HEADER.unlockRead(version, getMetadataAddress());
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
            return HEADER.lockWrite(version, blockID, offset, getMetadataAddress(), tap);
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
            return HEADER.unlockWrite(version, getMetadataAddress(),  tap);
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
    @Override
    public <K> int compareKeyAndSerializedKey(K key, OakScopedReadBuffer serializedKey, OakComparator<K> cmp) {
        if (((ScopedReadBuffer) serializedKey).s.lockRead() != ValueResult.TRUE) {
            throw new ErrorLockException();
        }
        int res = cmp.compareKeyAndSerializedKey(key, serializedKey);
        ((ScopedReadBuffer) serializedKey).s.unlockRead();
        return res;
    }
    
    @Override
    public <K> int compareSerializedKeys(OakScopedReadBuffer serializedKey1,
            OakScopedReadBuffer serializedKey2, OakComparator<K> cmp) {
        if (    ((ScopedReadBuffer) serializedKey1).s.lockRead() != ValueResult.TRUE ||
                ((ScopedReadBuffer) serializedKey2).s.lockRead() != ValueResult.TRUE) {
            throw new ErrorLockException();
        }
        int res = cmp.compareSerializedKeys(serializedKey1, serializedKey2);
        ((ScopedReadBuffer) serializedKey1).s.unlockRead();
        ((ScopedReadBuffer) serializedKey2).s.unlockRead();
        return res;
    }
}
