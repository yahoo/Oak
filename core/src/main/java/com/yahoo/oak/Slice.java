/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

// An abstract Slice represents an data about an off-heap cut: a portion of a bigger block,
// which is part of the underlying managed off-heap memory. Concrete implementation adapts
// the abstract Slice to a specific memory manager.
// Slice is allocated only via memory manager, and can be de-allocated later.
// Slice can be either empty or associated with an off-heap cut,
// which is the aforementioned portion of an off-heap memory.
abstract class Slice implements OakUnsafeDirectBuffer, Comparable<Slice> {
    static final int UNDEFINED_LENGTH_OR_OFFSET = -1;

    /** The fields describing the associated off-heap cut, they are set when slice is not empty **/
    protected long reference;
    protected int blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
    protected int offset  = UNDEFINED_LENGTH_OR_OFFSET;

    // The entire length of the off-heap cut, including the header!
    protected int length = UNDEFINED_LENGTH_OR_OFFSET;
    protected int version;    // Allocation time version
    protected ByteBuffer buffer = null;

    // true if slice is associated with an off-heap slice of memory
    // if associated is false the Slice is empty
    protected boolean associated = false;

    /* ------------------------------------------------------------------------------------
     * No Constructors, only for concrete implementations
     * ------------------------------------------------------------------------------------*/

    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    abstract Slice getDuplicatedSlice();

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
    abstract void invalidate();

    // initialize dummy for allocation
    void initializeLookupDummy(int l) {
        invalidate();
        length = l;
    }

    // Copy the block allocation information from another block allocation.
    void copyForAllocation(Slice other) {
        if (other == this) {
            // No need to do anything if the input is this object
            return;
        }
        this.blockID = other.blockID;
        this.offset = other.offset;
        this.length = other.length;
        this.buffer = other.buffer;
        this.reference = other.reference;
        this.associated = other.associated;
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

        // more Slice's properties are yet to be set by associateMMAllocation
    }

    /*
     * Updates everything that can be extracted with reference decoding,
     * including reference itself. This is not the full setting of the association
     */
    abstract void associateReferenceDecoding(int blockID, int offset, int arg, long reference);

    // Copy the block allocation information from another block allocation.
    abstract <T extends Slice> void copyFrom(T other);

    // Set the internal buffer.
    // This method should be used only within Memory Management package.
    void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        assert buffer != null;
        associated = true; // buffer is the final and the most important field for the slice validity
    }

    /*
     * Upon allocation, sets everything related to memory management of an off-heap cut:
     * a portion of a bigger block.
     * Used only within Memory Manager package.
     */
    abstract void associateMMAllocation(int version, long reference);

    // used only in case of iterations when the rest of the slice's data should remain the same
    // in this case once the offset is set the the slice is associated
    abstract void updateOnSameBlock(int offset, int length);

    // the method has no effect if length is already set
    protected abstract void prefetchDataLength();

    // simple setter, frequently internally used, to save code duplication
    protected void setBlockIdOffsetAndLength(int blockID, int offset, int length) {
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

    abstract int getAllocatedLength();

    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/
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
    public abstract int getOffset();

    @Override
    public abstract int getLength();

    @Override
    public long getAddress() {
        return ((DirectBuffer) buffer).address() + getOffset();
    }

    @Override
    public abstract String toString();

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

    /*-------------- Off-heap header operations: locking and logical delete --------------*/

    /**
     * Acquires a read lock
     *
     * @return {@code TRUE} if the read lock was acquires successfully
     * {@code FALSE} if the header/off-heap-cut is marked as deleted
     * {@code RETRY} if the header/off-heap-cut was moved, or the version of the off-heap header
     * does not match {@code version}.
     */
    abstract ValueUtils.ValueResult lockRead();

    /**
     * Releases a read lock
     *
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    abstract ValueUtils.ValueResult unlockRead();

    /**
     * Acquires a write lock
     *
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    abstract ValueUtils.ValueResult lockWrite();

    /**
     * Releases a write lock
     *
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    abstract ValueUtils.ValueResult unlockWrite();

    /**
     * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
     *
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    abstract ValueUtils.ValueResult logicalDelete();

    /**
     * Is the associated off-heap cut marked as logically deleted
     *
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    abstract ValueUtils.ValueResult isDeleted();

    /**
     * Marks the header of the associated off-heap cut as moved, just write (without CAS)
     * The write lock must be held (asserted inside the header)
     */
    abstract void markAsMoved();

    /**
     * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
     * The write lock must be held (asserted inside the header).
     * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
     */
    abstract void markAsDeleted();
}
