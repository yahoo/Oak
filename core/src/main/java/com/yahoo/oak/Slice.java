/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

// An abstract Slice represents an data about an off-heap cut: a portion of a bigger block,
// which is part of the underlying managed off-heap memory. Concrete implementation adapts
// the abstract Slice to a specific memory manager.
// Slice is allocated only via memory manager, and can be de-allocated later.
// Slice can be either empty or associated with an off-heap cut,
// which is the aforementioned portion of an off-heap memory.
abstract class Slice implements Comparable<Slice> {
    protected final int undefinedLengthOrOffsetOrAddress = -1;

    /** The fields describing the associated off-heap cut, they are set when slice is not empty **/
    protected long reference;
    protected int blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
    protected int offset  = undefinedLengthOrOffsetOrAddress;

    // The entire length of the off-heap cut, including the header!
    protected int length = undefinedLengthOrOffsetOrAddress;
    protected long memAddress = undefinedLengthOrOffsetOrAddress;

    // true if slice is associated with an off-heap slice of memory
    // if associated is false the Slice is empty
    protected boolean associated = false;

    /* ------------------------------------------------------------------------------------
     * No Constructors, only for concrete implementations. Memory Manager invocations below.
     * ------------------------------------------------------------------------------------*/

    /**
     * Allocate new off-heap cut and associated this slice with a new off-heap cut of memory
     *
     * @param size     the number of bytes required by the user
     * @param existing whether the allocation is for existing off-heap cut moving to the other
     *                 location (e.g. in order to be enlarged).
     */
    abstract void allocate(int size, boolean existing);

    /**
     * Release the associated off-heap cut, which is disconnected from the data structure,
     * but can be still accessed via threads previously having the access. It is the memory
     * manager responsibility to care for the old concurrent accesses.
     */
    abstract void release();

    /* ------------- Interfaces to deal with references! ------------- */
    /* Reference is a long (64 bits) that should encapsulate all the information required
     * to access a memory for read and for write. It is up to memory manager what to put inside.
     */

    /**
     * Decode information from reference to this Slice's fields.
     *
     * @param reference the reference to decode
     * @return true if the given allocation reference is valid and not deleted. If reference is
     * invalid, the slice is invalidated. If reference is deleted, this slice is updated anyway.
     */
    abstract boolean decodeReference(long reference);

    /**
     * Encode (create) the reference according to the information in this Slice
     *
     * @return the encoded reference
     */
    abstract long encodeReference();

    /* ------------------------------------------------------------------------------------
     * Getters for Reference encoding/decoding
     * ------------------------------------------------------------------------------------*/
    abstract long getThirdForReferenceEncoding();

    long getSecondForReferenceEncoding() {
        return getAllocatedOffset();
    }

    long getFirstForReferenceEncoding() {
        return getAllocatedBlockID();
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/
    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    abstract Slice getDuplicatedSlice();

    // Reset all common not final fields to invalid state
    abstract void invalidate();

    // initialize dummy for allocation
    void initializeLookupDummy(int l) {
        invalidate();
        length = l;
    }

    // Copy the block allocation information from another block allocation.
    void copyAllocationInfoFrom(Slice other) {
        if (other == this) {
            // No need to do anything if the input is this object
            return;
        }
        this.blockID = other.blockID;
        this.offset = other.offset;
        this.length = other.length;
        this.memAddress = other.memAddress;
        this.reference = other.reference;
        this.associated = other.associated;
    }

    /*
     * Sets everything related to allocation of an off-heap cut: a portion of a bigger block.
     * Turns empty slice to an associated slice upon allocation.
     */
    void associateBlockAllocation(int blockID, int offset, int length, long memAddress) {
        assert blockID != NativeMemoryAllocator.INVALID_BLOCK_ID
            && offset > undefinedLengthOrOffsetOrAddress
            && length > undefinedLengthOrOffsetOrAddress
            && memAddress != undefinedLengthOrOffsetOrAddress;
        setBlockIdOffsetAndLength(blockID, offset, length);
        this.memAddress  = memAddress;
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
    void setAddress(long memAddress) {
        this.memAddress = memAddress;
        assert memAddress != 0;
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
        return memAddress + offset;
    }

    public abstract int getLength();

    public abstract long getAddress();

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
