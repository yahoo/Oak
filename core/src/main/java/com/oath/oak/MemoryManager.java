/**
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

    public MemoryManager(OakMemoryAllocator valuesMemoryAllocator, OakMemoryAllocator keysMemoryAllocator) {
        assert valuesMemoryAllocator != null;
        assert keysMemoryAllocator != null;

        this.valuesMemoryAllocator = valuesMemoryAllocator;
        this.keysMemoryAllocator = keysMemoryAllocator;
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

    public ByteBuffer allocateKeys(int bytes) {
        return keysMemoryAllocator.allocate(bytes);
    }

    public OakNativeMemoryAllocator.Slice allocateSlice(int bytes) {
        return ((OakNativeMemoryAllocator)keysMemoryAllocator).allocateSlice(bytes);
    }

    public void releaseKeys(ByteBuffer keys) {
        // keys aren't going to be released until GC part is taken care for
    }

    // When some buffer need to be read from a random block
    public ByteBuffer readByteBufferFromBlockID(Integer id, int pos, int length) {
        return ((OakNativeMemoryAllocator)keysMemoryAllocator).
            readByteBufferFromBlockID(id, pos,length);
    }
}
