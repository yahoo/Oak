/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;



import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;


public class MemoryManager {
    private final OakMemoryAllocator keysMemoryAllocator;
    private final NettyMemoryAllocator valuesMemoryAllocator;

    public MemoryManager(OakMemoryAllocator keysMemoryAllocator) {
        this.valuesMemoryAllocator = new NettyMemoryAllocator();
        this.keysMemoryAllocator = keysMemoryAllocator;
    }

    public ByteBuf allocate(int size) {
        return valuesMemoryAllocator.allocate(size);
    }

    public void close() {
        keysMemoryAllocator.close();
    }

    void release(ByteBuf bb) {
        bb.release();
    }

    // how many memory is allocated for this OakMap
    public long allocated() {
        return valuesMemoryAllocator.allocated();
    }

    public ByteBuffer allocateKeys(int bytes) {
        return keysMemoryAllocator.allocate(bytes);
    }

    public void releaseKeys(ByteBuffer keys) {
        keysMemoryAllocator.free(keys);
    }
}
