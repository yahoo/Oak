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
        rcmm = new ReferenceCodecMM(BlocksPool.getInstance().blockSize(), this);
    }

    @Override
    public void close() {
        allocator.close();
    }

    @Override
    public boolean isClosed() {
        return allocator.isClosed();
    }

    @Override
    public int getCurrentVersion() {
        return globalVersionNumber.get();
    }

    /**
     * Get ReferenceCodec to manage the (long) references,
     * in which all the info for the memory access is incorporated
     *
     */
    @Override public ReferenceCodec getReferenceCodec() {
        return rcmm;
    }

    @Override
    public long allocated() {
        return allocator.allocated();
    }

    @Override
    public void allocate(Slice s, int size, Allocate allocate) {
        boolean allocated = allocator.allocate(s, size, allocate);
        assert allocated;
        s.setVersion(globalVersionNumber.get());
    }

    @Override
    public void release(Slice s) {
        s.getLength(); // to set the off-heap length if need, implicitly
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

    @Override
    public void readByteBuffer(Slice s, int blockID) {
        allocator.readByteBuffer(s, blockID);
    }
}
