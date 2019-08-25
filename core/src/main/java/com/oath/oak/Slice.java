/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

// Slice is a "small part" of a bigger block of the underlying managed memory.
// Slice is allocated for data (key or value) and can be de-allocated later
public class Slice {
    private final int blockID;
    private final ByteBuffer buffer;

    public Slice(int blockID, ByteBuffer buffer) {
        this.blockID = blockID;
        this.buffer = buffer;
    }

    public Slice(int blockID, int position, int length, MemoryManager memoryManager) {
        this(blockID, memoryManager.getByteBufferFromBlockID(blockID, position, length).duplicate());
    }

    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    public int getBlockID() {
        return blockID;
    }
}
