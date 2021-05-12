/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/* An abstract Slice represents the information about an off-heap cut: a portion of a bigger block,
** which is part of the underlying managed off-heap memory. Concrete implementation adapts
** the abstract Slice to a specific memory manager.
** Slice is allocated only via memory manager, and can be de-allocated later.
** Slice can be either empty or associated with an off-heap cut,
** which is the aforementioned portion of an off-heap memory.*/
abstract class AbstractSlice implements Slice, Comparable<AbstractSlice> {
    protected static final int UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS = -1;

    /** The fields describing the associated off-heap cut, they are set when slice is not empty **/
    protected long reference;
    protected int blockID;
    protected int offset;
    protected int length; // The entire length of the off-heap cut, including the header (if needed)!
    protected long memAddress;

    // true if slice is associated with an off-heap slice of memory
    // if associated is false the Slice is empty
    protected boolean associated;

    /* ------------------------------------------------------------------------------------
     * Protected Constructor
     * ------------------------------------------------------------------------------------*/
    protected AbstractSlice() {
        invalidate();
    }

    /**
     * Allocate new off-heap cut and associated this slice with a new off-heap cut of memory
     *
     * @param size     the number of bytes required by the user
     * @param existing whether the allocation is for existing off-heap cut moving to the other
     *                 location (e.g. in order to be enlarged).
     */
    public abstract void allocate(int size, boolean existing);

    /**
     * Release the associated off-heap cut, which is disconnected from the data structure,
     * but can be still accessed via threads previously having the access. It is the memory
     * manager responsibility to care for the old concurrent accesses.
     */
    public abstract void release();

    /**
     * Reset all Slice fields to invalid value, erase the previous association if existed.
     * This does not releases the associated off-heap cut to memory manager, just disconnects
     * the association!
     */
    public abstract void invalidate();

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
    public abstract boolean decodeReference(long reference);

    /* ------------------------------------------------------------------------------------
     * Slices duplication and info transfer
     * ------------------------------------------------------------------------------------*/

    /**
     * Used to duplicate the allocation state. Does not duplicate the underlying off-heap cut itself.
     * Should be used when ThreadContext's internal Slice needs to be exported to the user.
     */
    public abstract AbstractSlice duplicate();

    /**
     * Copy the off-heap cut allocation information from another off-heap cut allocation.
     */
    public abstract void copyFrom(Slice other);

    /**
     * Copy the common off-heap cut allocation information from another off-heap cut allocation.
     */
    protected void copyAllocationInfoFrom(AbstractSlice other) {
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

    // initialize dummy for allocation
    protected void initializeLookupDummy(int l) {
        invalidate();
        length = l;
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    public boolean isAssociated() {
        return associated;
    }

    public long getReference() {
        return reference;
    }

    public abstract int getLength();

    public abstract long getAddress();

    public abstract String toString();

    /* ------------------------------------------------------------------------------------
     * Common methods required internally by Memory Manager module
     * ------------------------------------------------------------------------------------*/

    /*
     * Sets everything related to allocation of an off-heap cut: a portion of a bigger block.
     * Turns empty slice to an associated slice upon allocation.
     */
    protected void associateBlockAllocation(int blockID, int offset, int length, long memAddress) {
        assert blockID != NativeMemoryAllocator.INVALID_BLOCK_ID
            && offset > UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS
            && length > UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS
            && memAddress != UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
        setBlockIdOffsetAndLength(blockID, offset, length);
        this.memAddress  = memAddress;
        // more Slice's properties are yet to be set by Memory Manager (MM) during allocation

        // needs to be set here (and not in MM) due to the tests,
        // which allocate and use Slice directly (not via MM)
        associated = true;
    }

    /* Set the internal memory address.
    * This method should be used only within Memory Management package.
    * */
    protected void setAddress(long memAddress) {
        assert memAddress != 0;
        this.memAddress = memAddress;
        // memAddress is the final and the most important field for the slice validity,
        // once it is set the slice is considered associated
        associated = true;
    }

    /*
     * Memory Manager's data created during allocation is set directly to the Slice.
     * This method is used only for testing!
     */
    @VisibleForTesting
    protected abstract void associateMMAllocation(int arg1, long arg2);

    // simple setter, frequently internally used, to save code duplication
    protected void setBlockIdOffsetAndLength(int blockID, int offset, int length) {
        this.blockID = blockID;
        this.offset = offset;
        this.length = length;
    }

    /* ------------------------------------------------------------------------------------
     * Internal information getters
     * ------------------------------------------------------------------------------------*/

    /* BlockID should be used only internally within MM package */
    protected int getAllocatedBlockID() {
        return blockID;
    }

    /* Offset within the block should be used only internally within MM package */
    protected int getAllocatedOffset() {
        return offset;
    }

    /* Returns the length of the off-heap cut including the space allocated for metadata (if any) */
    protected abstract int getAllocatedLength();

    /*
     * Returns the address of the absolute beginning of the off-heap cut,
     * where metadata may be located.
     **/
    protected long getMetadataAddress() {
        assert associated;
        return memAddress + offset;
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
    public int compareTo(AbstractSlice o) {
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
    public abstract ValueUtils.ValueResult lockRead();

    /**
     * Releases a read lock
     *
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    public abstract ValueUtils.ValueResult unlockRead();

    /**
     * Acquires a write lock
     *
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    public abstract ValueUtils.ValueResult lockWrite();

    /**
     * Releases a write lock
     *
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    public abstract ValueUtils.ValueResult unlockWrite();

    /**
     * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
     *
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    public abstract ValueUtils.ValueResult logicalDelete();

    /**
     * Is the associated off-heap cut marked as logically deleted
     *
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    public abstract ValueUtils.ValueResult isDeleted();

    /**
     * Marks the header of the associated off-heap cut as moved, just write (without CAS)
     * The write lock must be held (asserted inside the header)
     */
    public abstract void markAsMoved();

    /**
     * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
     * The write lock must be held (asserted inside the header).
     * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
     */
    public abstract void markAsDeleted();
}
