/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

import static com.oath.oak.NovaValueOperationsImpl.LockStates.FREE;


// Slice is a "small part" of a bigger block of the underlying managed memory.
// Slice is allocated for data (key or value) and can be de-allocated later
public class Slice {
    private final int blockID;
    private ByteBuffer buffer;

    public Slice(int blockID, ByteBuffer buffer) {
        this.blockID = blockID;
        this.buffer = buffer;
    }

    public Slice(int blockID, int position, int length, NovaManager memoryManager) {
        this(blockID, memoryManager.getByteBufferFromBlockID(blockID, position, length).duplicate());
    }

    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    public int getBlockID() {
        return blockID;
    }

    void initHeader(NovaValueUtils operator) {
        buffer.putInt(buffer.position() + operator.getLockLocation(), FREE.value);
    }

    public Slice readOnly() {
        buffer = buffer.asReadOnlyBuffer();
        return this;
    }

    public Slice duplicate() {
        return new Slice(blockID, buffer.duplicate());
    }
}
