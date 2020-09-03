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
    protected long reference;
    protected int blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
    protected int offset = UNDEFINED_LENGTH_OR_OFFSET;

    // The entire length of slice including header!
    protected int length = UNDEFINED_LENGTH_OR_OFFSET;

    // Allocation time version
    protected int version;

    protected ByteBuffer buffer = null;

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
        blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
        reference   = ReferenceCodecMM.getInvalidReference();
        length      = UNDEFINED_LENGTH_OR_OFFSET;
        offset      = UNDEFINED_LENGTH_OR_OFFSET;
        buffer      = null;
        valid       = false;
    }

    // initialize dummy for allocation
    void initializeDummy(int l) {
        invalidate();
        length = l;
    }

    /*
     * Updates the offset and length of the slice.
     * The buffer and its blockID should be set later by the block allocator (via setBufferAndBlockID).
     */
    void setBlockidAndOffsetAndLength(int blockID, int offset, int length) {
        if (length != UNDEFINED_LENGTH_OR_OFFSET) {
            // length can remain undefined until requested
            // given length should include the header
            assert headerSize <= length;
        }
        this.blockID = blockID;
        this.offset = offset;
        this.length = length;
        valid = false;
    }

    /*
     * Updates the offset, length and version of the slice
     * The buffer and its blockID should be set later by the block allocator (via setBufferAndBlockID).
     */
    void setBlockidOffsetLengthAndVersion(int blockID, int offset, int length, int version) {
        setBlockidAndOffsetAndLength(blockID, offset, length);
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
        this.reference = other.reference;
    }

    // Set the internal buffer.
    // This method should be used only by the block memory allocator.
    void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        valid = true; // buffer is the final and the most important field for the slice validity
    }

    // Set the version. Should be used by the memory allocator.
    void setVersion(int version) {
        this.version = version;
    }

    long getReference() {
        return reference;
    }

    void setReference(long reference) {
        this.reference = reference;
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    boolean isInitiated() {
        return valid;
    }

    int getAllocatedBlockID() {
        return blockID;
    }

    int getAllocatedOffset() {
        return offset;
    }

    int getAllocatedLength() {
        assert valid;
        if (length == UNDEFINED_LENGTH_OR_OFFSET) {
            ValueUtilsImpl.setLengthFromOffHeap(this);
        }
        return length;
    }

    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/
    int getVersion() {
        return version;
    }

    long getMetadataAddress() {
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
        if (length == UNDEFINED_LENGTH_OR_OFFSET) {
            ValueUtilsImpl.setLengthFromOffHeap(this);
        }
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
