/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;


class NoFreeMemoryManager implements MemoryManager {
    private final OakBlockMemoryAllocator keysMemoryAllocator;
    private final OakBlockMemoryAllocator valuesMemoryAllocator;

    NoFreeMemoryManager(OakBlockMemoryAllocator memoryAllocator) {
        assert memoryAllocator != null;

        this.valuesMemoryAllocator = memoryAllocator;
        this.keysMemoryAllocator = memoryAllocator;
    }

    public void close() {
        valuesMemoryAllocator.close();
        keysMemoryAllocator.close();
    }

    // how many memory is allocated for this OakMap
    public long allocated() {
        return valuesMemoryAllocator.allocated();
    }

    @Override
    public Slice allocateSlice(int size, Allocate allocate) {
        return keysMemoryAllocator.allocateSlice(size, allocate);
    }

    public Slice allocateSlice(int size) {
        return allocateSlice(size, MemoryManager.Allocate.KEY);
    }

    @Override
    public void releaseSlice(Slice slice) {
    }

    @Override
    public ByteBuffer getByteBufferFromBlockID(int blockID, int bufferPosition, int bufferLength) {
        return keysMemoryAllocator.readByteBufferFromBlockID(
                blockID, bufferPosition, bufferLength);
    }

    public boolean isClosed() {
        return keysMemoryAllocator.isClosed() || valuesMemoryAllocator.isClosed();
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }
}

