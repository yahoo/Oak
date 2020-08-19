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
    static final int UNDEFINED_LENGTH_OR_OFFSET = 0;

    /**
     * An allocated slice might have reserved space for meta-data, i.e., a header.
     * In the current implementation, the header size is defined externally at the construction.
     * In future implementations, the header size should be part of the allocation and defined
     * by the allocator/memory-manager using the update() method.
     */
    protected final int headerSize;

    protected int blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
    protected int offset = UNDEFINED_LENGTH_OR_OFFSET;

    // The entire length of slice including header!
    protected int length = UNDEFINED_LENGTH_OR_OFFSET;

    // Allocation time version
    protected int version;

    protected ByteBuffer buffer;

    private boolean valid = false;

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
        valid = false;
//        blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
//        offset = UNDEFINED_LENGTH_OR_OFFSET;
//        length = UNDEFINED_LENGTH_OR_OFFSET;
//        version = ReferenceCodecMM.INVALID_VERSION;
//        buffer = null;
    }

    /*
     * Updates the allocation object.
     * The buffer should be set later by the block allocator.
     */
    void update(int blockID, int offset, int length) {
        if (length != UNDEFINED_LENGTH_OR_OFFSET) {
            // length can remain undefined until requested
            // given length should include the header
            assert headerSize <= length;
        }

        //this.blockID = blockID;
        this.offset = offset;
        this.length = length;

        // Invalidate the buffer and version. Will be assigned by the allocator.
        this.version = ReferenceCodecMM.INVALID_VERSION;
        //this.buffer = null;
        valid = true;
    }

    /*
     * Updates the allocation object, including version.
     * The buffer should be set later by the block allocator.
     */
    void update(int blockID, int offset, int length, int version) {
        update(blockID, offset, length);
        this.version = version;
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
        this.valid = other.valid;
    }

    // Set the internal buffer.
    // This method should be used only by the block memory allocator.
    void setBuffer(ByteBuffer buffer, int blockID) {
        this.blockID = blockID;
        this.buffer = buffer;
        updateLengthFromOffHeapIfNeeded();
    }

    // Set the version. Should be used by the memory allocator.
    void setVersion(int version) {
        this.version = version;
    }

    private void updateLengthFromOffHeapIfNeeded() {
        if (length == UNDEFINED_LENGTH_OR_OFFSET) {
            // get it from off-heap header

            ValueUtilsImpl.setLengthFromOffHeap(this);

        }
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    boolean isValid() {
        return valid;
       // return blockID != NativeMemoryAllocator.INVALID_BLOCK_ID;
    }

    int getAllocatedBlockID() {
        return blockID;
    }

    int getAllocatedOffset() {
        return offset;
    }

    int getAllocatedLength() {
        assert valid;
        updateLengthFromOffHeapIfNeeded();
        return length;
    }

    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/
    int getVersion() {
        return version;
    }

    long getMetadataAddress() {
        assert buffer != null;
        assert valid;
        return ((DirectBuffer) buffer).address() + offset;
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        assert valid;
        return buffer;
    }

    @Override
    public int getOffset() {
        assert valid;
        return offset + headerSize;
    }

    @Override
    public int getLength() {
        updateLengthFromOffHeapIfNeeded();
        return length - headerSize;
    }

    void setDataLength(int length) {
        // the length kept in header is the length of the data only!
        // add header size
        this.length = length + headerSize;
    }

    @Override
    public long getAddress() {
        return ((DirectBuffer) buffer).address() + getOffset();
    }

    @Override
    public String toString() {
        return String.format("Slice(blockID=%d, offset=%,d, length=%,d, version=%d)", blockID, offset, length, version);
    }

    /*-------------- Comparable<Slice> --------------*/

    /**
     * The slices are ordered by their length, then by their block id, then by their offset.
     * Slices with the same length, block id and offset are considered identical.
     *
     * Used for comparision of the slices in the freeList of memory manager,
     * the slices are deleted but not yet re-allocated.
     */
    @Override
    public int compareTo(Slice o) {
        updateLengthFromOffHeapIfNeeded(); // without reallocation old length is still valid
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
