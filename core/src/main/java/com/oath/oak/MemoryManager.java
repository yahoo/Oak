/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;



import io.netty.buffer.ByteBuf;

public class MemoryManager {
    private final NettyMemoryAllocator keysMemoryAllocator;
    private final NettyMemoryAllocator valuesMemoryAllocator;

    public MemoryManager(OakMemoryAllocator keysMemoryAllocator) {
        NettyMemoryAllocator memoryManager = new NettyMemoryAllocator();
        this.valuesMemoryAllocator =memoryManager;
        this.keysMemoryAllocator = memoryManager;
    }

    public ByteBuf allocate(int size) {
        return valuesMemoryAllocator.allocate(size);
    }

    public void close() {

    }

    void release(ByteBuf bb) {
        bb.release();
    }

    // how many memory is allocated for this OakMap
    public long allocated() {
        return valuesMemoryAllocator.allocated();
    }

    public ByteBuf allocateKeys(int bytes) {
        return keysMemoryAllocator.allocate(bytes);
    }

    public void releaseKeys(ByteBuf keys) {
        keys.release();
    }
}
