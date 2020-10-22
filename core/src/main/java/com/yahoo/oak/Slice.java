/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

// Slice represents an data about an off-heap cut: a portion of a bigger block,
// which is part of the underlying managed off-heap memory.
// Slice is allocated only via memory manager, and can be de-allocated later.
// Slice can be either empty or associated with an off-heap cut,
// which is the aforementioned portion of an off-heap memory.
class Slice implements OakUnsafeDirectBuffer, Comparable<Slice> {
    static final int UNDEFINED_LENGTH_OR_OFFSET = -1;

    /**
     * An allocated off-heap cut might have reserved space for meta-data, i.e., a header.
     * The header size is defined externally by memory-manager at the slice construction.
     */
    protected final int headerSize;

    /** The fields describing the associated off-heap cut, they are set when slice is not empty **/
    protected long reference;
    private final long invalidReferenceValue; // used for invalidation
    protected int blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
    protected int offset  = UNDEFINED_LENGTH_OR_OFFSET;

    // The entire length of the off-heap cut, including the header!
    protected int length = UNDEFINED_LENGTH_OR_OFFSET;
    private int version;    // Allocation time version
    private ByteBuffer buffer = null;

    // true if slice is associated with an off-heap slice of memory
    // if associated is false the Slice is empty
    private boolean associated = false;

    /* ------------------------------------------------------------------------------------
     * Constructors
     * ------------------------------------------------------------------------------------*/
    // Should be used only by Memory Manager (within Memory Manager package)
    Slice(int headerSize, long invalidReferenceValue) {
        this.headerSize = headerSize;
        this.invalidReferenceValue = invalidReferenceValue;
        invalidate();
    }

    // Should be used only for testing
    @VisibleForTesting
    Slice() {
        this(0, 0);
    }

    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    Slice getDuplicatedSlice() {
        Slice newSlice = new Slice(this.headerSize, this.invalidReferenceValue);
        newSlice.copyFrom(this);
        return newSlice;
    }

    // Used only in testing to make each slice reference different buffer object,
    // but the same off-heap memory
    @VisibleForTesting
    void duplicateBuffer() {
        buffer = buffer.duplicate();
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/
    // Reset all not final fields to invalid state
    void invalidate() {
        blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
        reference   = invalidReferenceValue;
        length      = UNDEFINED_LENGTH_OR_OFFSET;
        offset      = UNDEFINED_LENGTH_OR_OFFSET;
        buffer      = null;
        associated  = false;
    }

    // initialize dummy for allocation
    void initializeLookupDummy(int l) {
        invalidate();
        length = l;
    }

    /*
     * Sets everything related to allocation of an off-heap cut: a portion of a bigger block.
     * Turns empty slice to an associated slice upon allocation.
     */
    void associateBlockAllocation(int blockID, int offset, int length, ByteBuffer buffer){
        assert blockID != NativeMemoryAllocator.INVALID_BLOCK_ID
            && offset > UNDEFINED_LENGTH_OR_OFFSET && length > UNDEFINED_LENGTH_OR_OFFSET
            && buffer != null;
        setBlockIdOffsetAndLength(blockID, offset, length);
        this.buffer  = buffer;
        this.associated = true;

        // reference and (maybe) version are yet to be set by associateMMAllocation
    }

    /*
     * Updates everything that can be extracted with reference decoding of "NoFree" type,
     * including reference itself. The separation by reference decoding type is temporary!
     * This is not the full setting of the association, therefore 'associated' flag remains false
     */
    void associateReferenceDecodingNoFree(int blockID, int offset, int length, long reference) {
        // length can remain undefined until requested, but if given length should include the header
        assert length != UNDEFINED_LENGTH_OR_OFFSET && headerSize <= length;
        setBlockIdOffsetAndLength(blockID, offset, length);
        this.reference = reference;
        associated   = false;
    }

    /*
     * Updates everything that can be extracted with reference decoding of "Native" type,
     * including reference itself. The separation by reference decoding type is temporary!
     * This is not the full setting of the association, therefore 'associated' flag remains false
     */
    void associateReferenceDecodingNative(int blockID, int offset, int version, long reference) {
        setBlockIdOffsetAndLength(blockID, offset, UNDEFINED_LENGTH_OR_OFFSET);
        this.reference = reference;
        this.version = version;
        associated   = false;
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
        this.reference = other.reference;
        this.associated = other.associated;
    }

    // Set the internal buffer.
    // This method should be used only within Memory Management package.
    void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        assert buffer != null;
        associated = true; // buffer is the final and the most important field for the slice validity
    }

    /*
     * Upon allocstion, sets everything related to memory management of an off-heap cut:
     * a portion of a bigger block.
     * Used only within Memory Manager package.
     */
    void associateMMAllocation(int version, long reference) {
        this.version = version;
        this.reference = reference;
    }

    // used only in case of iterations when the rest of the slice's data should remain the same
    // in this case once the offset is set the the slice is associated
    void setIterationOnSameBlock(int offset, int length) {
        this.offset = offset;
        this.length = length;
        assert buffer != null;
        this.associated = true;
    }

    void setDataLength(int length) {
        // the length kept in header is the length of the data only!
        // add header size
        this.length = length + headerSize;
    }

    // simple setter, frequently internally used, to save code duplication
    private void setBlockIdOffsetAndLength(int blockID, int offset, int length) {
        this.blockID = blockID;
        this.offset = offset;
        this.length = length;
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    boolean isInitiated() {
        return associated;
    }

    long getReference() {
        return reference;
    }

    int getAllocatedBlockID() {
        return blockID;
    }

    int getAllocatedOffset() {
        return offset;
    }

    int getAllocatedLength() {
        assert associated;
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
        assert associated;
        return ((DirectBuffer) buffer).address() + offset;
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        assert associated;
        return buffer;
    }

    @Override
    public int getOffset() {
        assert associated;
        return offset + headerSize;
    }

    @Override
    public int getLength() {
        if (length == UNDEFINED_LENGTH_OR_OFFSET) {
            ValueUtilsImpl.setLengthFromOffHeap(this);
        }
        return length - headerSize;
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
