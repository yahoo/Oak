/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;



import java.nio.ByteBuffer;


public class MemoryManager {
    private final OakMemoryAllocator keysMemoryAllocator;
    private final OakMemoryAllocator memoryAllocator;

    public MemoryManager(OakMemoryAllocator ma) {
        assert ma != null;

        this.memoryAllocator = ma;
        keysMemoryAllocator = new DirectMemoryAllocator();
    }

    public ByteBuffer allocate(int size) {
        return memoryAllocator.allocate(size);
    }

    public void close() {
        memoryAllocator.close();
        keysMemoryAllocator.close();
    }

    void release(ByteBuffer bb) {
        memoryAllocator.free(bb);
    }

    // how many memory is allocated for this OakMap
    public long allocated() {
        return memoryAllocator.allocated();
    }

    public ByteBuffer allocateKeys(int bytes) {
        return keysMemoryAllocator.allocate(bytes);
    }

    public void releaseKeys(ByteBuffer keys) {
        keysMemoryAllocator.free(keys);
    }
}
