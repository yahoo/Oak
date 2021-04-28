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
    private final List<List<SliceSyncRecycle>> releaseLists;
    private final AtomicInteger globalVersionNumber;
    private final BlockMemoryAllocator allocator;

    /*
     * The VALUE_RC reference codec encodes the reference (with memory manager abilities) of the values
     * into a single long primitive (64 bit).
     * For encoding details please take a look on ReferenceCodecSyncRecycle
     *
     */
    private final ReferenceCodecSyncRecycle rcmm;

    SyncRecycleMemoryManager(BlockMemoryAllocator allocator) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalVersionNumber = new AtomicInteger(VERS_INIT_VALUE);
        this.allocator = allocator;
        rcmm = new ReferenceCodecSyncRecycle(BlocksPool.getInstance().blockSize(), allocator);
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
    int getCurrentVersion() {
        return globalVersionNumber.get();
    }

    /**
     * Information from reference to slice
     * @param s         the memory slice to update with the info decoded from the reference
     * @param reference the reference to decode
     * @return true if the given allocation reference is valid, otherwise the slice is invalidated
     */
    boolean decodeReference(SliceSyncRecycle s, long reference) {
        // reference is set in the slice as part of decoding
        if (rcmm.decode(s, reference)) {
            allocator.readMemoryAddress(s);
            return true;
        }
        return false;
    }

    /**
     * Present the reference as it needs to be when the target is deleted
     *
     * @param reference to alter
     * @return the encoded reference
     */
    @Override
    public long alterReferenceForDelete(long reference) {
        return rcmm.alterForDelete(reference);
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
        return rcmm.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceDeleted(long reference) {
        return rcmm.isReferenceDeleted(reference);
    }

    @Override public boolean isReferenceValidAndNotDeleted(long reference) {
        return rcmm.isReferenceValidAndNotDeleted(reference);
    }

    @Override
    public boolean isReferenceConsistent(long reference) {
        return rcmm.isReferenceConsistent(reference);
    }

    @Override
    public SliceSyncRecycle getEmptySlice() {
        return new SliceSyncRecycle(OFF_HEAP_HEADER_SIZE, HEADER, this, rcmm);
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

    // 1. Native memory manager requires metadata header to be placed before the user data written
    // off-heap, therefore the bigger than requested size is allocated
    // 2. The parameter flag existing explains whether the allocation is for existing slice
    // moving to the other location (e.g. in order to be enlarged). The algorithm of move requires
    // the lock of the newly allocated slice to be taken exclusively until the process of move is finished.
    long allocate(SliceSyncRecycle s, int size, boolean existing) {
        boolean allocated = allocator.allocate(s, size + OFF_HEAP_HEADER_SIZE);
        assert allocated;
        int allocationVersion = globalVersionNumber.get();
        s.setVersion(allocationVersion);
        // Initiate the header that is serving for synchronization and memory management
        // for value written for the first time (not existing):
        //      initializing the header's lock to be free
        // for value being moved (existing): initialize the lock to be locked
        if (existing) {
            HEADER.initLockedHeader(s.getMetadataAddress(), size, allocationVersion);
        } else {
            HEADER.initFreeHeader(s.getMetadataAddress(), size, allocationVersion);
        }
        assert HEADER.getOffHeapVersion(s.getMetadataAddress()) == allocationVersion;
        return s.encodeReference();
    }

    /**
     * When returning an allocated Slice to the Memory Manager, depending on the implementation, there might be a
     * restriction on whether this allocation is reachable by other threads or not.
     *
     * @param s the allocation object to release
     */
    void release(SliceSyncRecycle s) {
        s.prefetchDataLength(); // this will set the length from off-heap header, if needed
        int idx = threadIndexCalculator.getIndex();
        List<SliceSyncRecycle> myReleaseList = this.releaseLists.get(idx);
        // ensure the length of the slice is always set
        myReleaseList.add(s.getDuplicatedSlice());
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            increaseGlobalVersion();
            for (SliceSyncRecycle allocToRelease : myReleaseList) {
                allocator.free(allocToRelease);
            }
            myReleaseList.clear();
        }
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
}
