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
    // version with which this slice was allocated, if slice is not a result of a new creation the version can be invalid
    private int version;
    // This field is only used for sanity checks purposes and it should not be used, nor changed.
    private final int originalPosition;

    public Slice(int blockID, ByteBuffer buffer) {
        this.blockID = blockID;
        this.buffer = buffer;
        this.originalPosition = buffer.position();
        this.version = ValueUtilsImpl.INVALID_VERSION;
    }

    Slice(int blockID, int position, int length, MemoryManager memoryManager) {
        this(blockID, memoryManager.getByteBufferFromBlockID(blockID, position, length));
    }

    public Slice duplicate() {
        return new Slice(blockID, buffer.duplicate());
    }

    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    int getBlockID() {
        return blockID;
    }

    boolean validatePosition() {
        return originalPosition == buffer.position();
    }

    int getOriginalPosition() { return originalPosition; }

    void setVersion(int version) {this.version = version;}

    int getVersion() { return version; }
}
