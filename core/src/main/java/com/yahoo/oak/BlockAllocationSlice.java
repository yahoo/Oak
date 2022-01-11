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
abstract class BlockAllocationSlice implements Slice, Comparable<BlockAllocationSlice> {
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
    protected BlockAllocationSlice() {
        invalidate();
    }

    /* ------------------------------------------------------------------------------------
     * Slices duplication and info transfer
     * ------------------------------------------------------------------------------------*/

    /**
     * Used to duplicate the allocation state. Does not duplicate the underlying off-heap cut itself.
     * Should be used when ThreadContext's internal Slice needs to be exported to the user.
     */
    public abstract BlockAllocationSlice duplicate();

    /**
     * Copy the common off-heap cut allocation information from another off-heap cut allocation.
     */
    protected void copyAllocationInfoFrom(BlockAllocationSlice other) {
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

    // zero the underlying memory (not the header) before entering the free list
    protected abstract void zeroMetadata();

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
    public int compareTo(BlockAllocationSlice o) {
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
