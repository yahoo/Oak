/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

// Represents a portion of a bigger block which is part of the underlying managed memory.
// It is allocated via block memory allocator, and can be de-allocated later
class Slice implements OakUnsafeDirectBuffer, Comparable<Slice> {

    /**
     * An allocated slice might have reserved space for meta-data, i.e., a header.
     * In the current implementation, the header size is defined externally at the construction.
     * In future implementations, the header size should be part of the allocation and defined
     * by the allocator/memory-manager using the update() method.
     */
    protected final int headerSize;

    protected int blockID;
    protected int offset;
    protected int length;

    // Allocation time version
    protected int version;

    protected ByteBuffer buffer;

    Slice(int headerSize) {
        this.headerSize = headerSize;
        invalidate();
    }

    // Should be used only for testing
    Slice() {
        this(0);
    }

    // Used to duplicate the allocation state. Does not duplicate the buffer itself.
    Slice(Slice otherSlice) {
        this.headerSize = otherSlice.headerSize;
        copyFrom(otherSlice);
    }

    // Used by OffHeapList in "synchrobench" module, and for testings.
    void duplicateBuffer() {
        buffer = buffer.duplicate();
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/

    // Reset to invalid state
    void invalidate() {
        blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
        offset = -1;
        length = -1;
        version = EntrySet.INVALID_VERSION;
        buffer = null;
    }

    /*
     * Updates the allocation object.
     * The buffer should be set later by the block allocator.
     */
    void update(int blockID, int offset, int length) {
        assert headerSize <= length;

        this.blockID = blockID;
        this.offset = offset;
        this.length = length;

        // Invalidate the buffer and version. Will be assigned by the allocator.
        this.version = EntrySet.INVALID_VERSION;
        this.buffer = null;
    }

    // Copy the block allocation information from another block allocation.
    void copyFrom(Slice other) {
        if (other == this) {
            // No need to do anything if the input is this object
            return;
        }
        this.blockID = other.blockID;
        this.offset = other.offset;
        this.length = other.length;
        this.version = other.version;
        this.buffer = other.buffer;
    }

    // Set the internal buffer.
    // This method should be used only by the block memory allocator.
    void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    // Set the version. Should be used by the memory allocator.
    void setVersion(int version) {
        this.version = version;
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    boolean isAllocated() {
        return blockID != NativeMemoryAllocator.INVALID_BLOCK_ID;
    }

    int getAllocatedBlockID() {
        return blockID;
    }

    int getAllocatedOffset() {
        return offset;
    }

    int getAllocatedLength() {
        return length;
    }

    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/

    boolean isValidVersion() {
        return version != EntrySet.INVALID_VERSION;
    }

    int getVersion() {
        return version;
    }

    long getMetadataAddress() {
        return ((DirectBuffer) buffer).address() + offset;
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    @Override
    public int getOffset() {
        return offset + headerSize;
    }

    @Override
    public int getLength() {
        return length - headerSize;
    }

    @Override
    public long getAddress() {
        return ((DirectBuffer) buffer).address() + getOffset();
    }

    /*-------------- Comparable<Slice> --------------*/

    /**
     * The slices are ordered by their length, then by their block id, then by their offset.
     * Slices with the same length, block id and offset are considered identical.
     */
    @Override
    public int compareTo(Slice o) {
        int cmp = Integer.compare(this.length, o.length);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.blockID, o.blockID);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.offset, o.offset);
    }
}
