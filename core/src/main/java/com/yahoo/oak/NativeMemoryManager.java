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

class NativeMemoryManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    private final ThreadIndexCalculator threadIndexCalculator;
    private final List<List<Slice>> releaseLists;
    private final AtomicInteger globalVersionNumber;
    private final BlockMemoryAllocator allocator;

    /*
     * The VALUE_RC reference codec encodes the reference (with memory manager abilities) of the values
     * into a single long primitive (64 bit).
     * For encoding details please take a look on ReferenceCodecMM
     *
     */
    private final ReferenceCodecMM rcmm;

    NativeMemoryManager(BlockMemoryAllocator allocator) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalVersionNumber = new AtomicInteger(1);
        this.allocator = allocator;
        rcmm = new ReferenceCodecMM(BlocksPool.getInstance().blockSize(), allocator);
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
     * @param s         the memory slice to update with the info decoded from the reference
     * @param reference the reference to decode
     * @return true if the given allocation reference is valid, otherwise the slice is invalidated
     */
    @Override
    public boolean decodeReference(Slice s, long reference) {
        s.setReference(reference);
        if (rcmm.decode(s, reference)) {
            allocator.readByteBuffer(s);
            return true;
        }
        return false;
    }

    /**
     * @param s the memory slice, encoding of which should be returned as a an output long reference
     * @return the encoded reference
     */
    @Override
    public long encodeReference(Slice s) {
        return rcmm.encode(s);
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

    @Override
    public boolean isReferenceValid(long reference) {
        return rcmm.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceDeleted(long reference) {
        return rcmm.isReferenceDeleted(reference);
    }

    @Override
    public boolean isReferenceConsistent(long reference) {
        return rcmm.isReferenceConsistent(reference);
    }

    @Override
    public long allocated() {
        return allocator.allocated();
    }

    @Override
    public void allocate(Slice s, int size) {
        boolean allocated = allocator.allocate(s, size);
        assert allocated;
        s.setVersion(globalVersionNumber.get());
    }

    @Override
    public void release(Slice s) {
        if (s.length == Slice.UNDEFINED_LENGTH_OR_OFFSET) {
            ValueUtilsImpl.setLengthFromOffHeap(s);
        }
        int idx = threadIndexCalculator.getIndex();
        List<Slice> myReleaseList = this.releaseLists.get(idx);
        // ensure the length of the slice is always set
        myReleaseList.add(new Slice(s));
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            globalVersionNumber.incrementAndGet();
            for (Slice allocToRelease : myReleaseList) {
                allocator.free(allocToRelease);
            }
            myReleaseList.clear();
        }
    }
}
