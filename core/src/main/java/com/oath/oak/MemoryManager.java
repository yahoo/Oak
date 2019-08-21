/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import java.nio.ByteBuffer;


public class MemoryManager {
    private final OakMemoryAllocator keysMemoryAllocator;
    private final OakMemoryAllocator valuesMemoryAllocator;

    public MemoryManager(OakMemoryAllocator memoryAllocator) {
        assert memoryAllocator != null;

        this.valuesMemoryAllocator = memoryAllocator;
        this.keysMemoryAllocator = memoryAllocator;
    }

    public ByteBuffer allocate(int size) {
        return valuesMemoryAllocator.allocate(size);
    }

    public void close() {
        valuesMemoryAllocator.close();
        keysMemoryAllocator.close();
    }

    void release(ByteBuffer bb) {
        valuesMemoryAllocator.free(bb);
    }

    // how many memory is allocated for this OakMap
    public long allocated() {
        return valuesMemoryAllocator.allocated();
    }

    // allocateSlice is used when the blockID (of the block from which the ByteBuffer is allocated)
    // needs to be known. Currently allocateSlice() is used for keys and
    // allocate() is used for values.
    public Slice allocateSlice(int bytes) {
        return ((OakNativeMemoryAllocator)keysMemoryAllocator).allocateSlice(bytes);
    }

    public void releaseSlice(Slice slice) {
        // keys aren't going to be released until GC part is taken care for
        ((OakNativeMemoryAllocator)keysMemoryAllocator).freeSlice(slice);
    }

    // When some read only buffer needs to be read from a random block
    public ByteBuffer getByteBufferFromBlockID(Integer BlockID, int bufferPosition, int bufferLength) {
        return ((OakNativeMemoryAllocator)keysMemoryAllocator).readByteBufferFromBlockID(
            BlockID, bufferPosition, bufferLength);
    }
}

