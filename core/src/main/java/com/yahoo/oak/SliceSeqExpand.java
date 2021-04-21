/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * SliceSeqExpand represents an data about an off-heap cut: a portion of a bigger block,
 * which is part of the underlying managed off-heap memory, which is managed for sequential
 * access and without memory recycling.
 * SliceSeqExpand is allocated only via SeqExpandMemoryManager, and can be de-allocated later.
 * Any slice can be either empty or associated with an off-heap cut,
 * which is the aforementioned portion of an off-heap memory.
 */
class SliceSeqExpand extends Slice {
    static final int UNDEFINED_LENGTH_OR_OFFSET = -1;

    /* ------------------------------------------------------------------------------------
     * Constructors
     * ------------------------------------------------------------------------------------*/
    // Should be used only by Memory Manager (within Memory Manager package)
    SliceSeqExpand() {
        super();
        invalidate();
    }

    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    SliceSeqExpand getDuplicatedSlice() {
        SliceSeqExpand newSlice = new SliceSeqExpand();
        newSlice.copyFrom(this);
        return newSlice;
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/

    /*
     * Updates everything that can be extracted with reference decoding of "SeqExpand" type,
     * including reference itself.
     * This is not the full setting of the association, therefore 'associated' flag remains false
     */
    void associateReferenceDecoding(int blockID, int offset, int length, long reference) {
        // length can remain undefined until requested, but if given length should include the header
        assert length != UNDEFINED_LENGTH_OR_OFFSET;
        setBlockIdOffsetAndLength(blockID, offset, length);
        this.reference = reference;
        associated   = false;
    }

    // Copy the block allocation information from another block allocation.
    <T extends Slice> void copyFrom(T other) {
        if (other instanceof SliceSeqExpand) { //TODO: any other ideas?
            copyAllocationInfoFrom(other);
            // if SliceSeqExpand gets new members (not included in allocation info)
            // their copy needs to be added here
        } else {
            throw new IllegalStateException("Must provide SliceSeqExpand other Slice - fake change");
        }
    }

    /*
     * Upon allocation, sets everything related to Sequential-Expending memory management of
     * an off-heap cut: a portion of a bigger block.
     * Used only within Memory Manager package.
     */
    void associateMMAllocation(int arg, long reference) {
        this.reference = reference;
    }

    // used only in case of iterations when the rest of the slice's data should remain the same
    // in this case once the offset is set the the slice is associated
    void updateOnSameBlock(int offset, int length) {
        this.offset = offset;
        this.length = length;
        assert memAddress != UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
        this.associated = true;
    }

    // the method has no effect for SliceSeqExpand
    protected void prefetchDataLength() {
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/
    int getAllocatedLength() {
        assert associated;
        return length;
    }

    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/


    @Override
    public int getLength() {
        return length;
    }

    @Override
    public long getAddress() {
        return memAddress + offset;
    }

    @Override
    public String toString() {
        return String.format("SliceSeqExpand(blockID=%d, offset=%,d, length=%,d)",
            blockID, offset, length);
    }

    /*-------------- Off-heap header operations: locking and logical delete --------------*/

    /**
     * Acquires a read lock.
     * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
     *
     * @return {@code TRUE} if the read lock was acquires successfully
     * {@code FALSE} if the header/off-heap-cut is marked as deleted
     * {@code RETRY} if the header/off-heap-cut was moved, or the version of the off-heap header
     * does not match {@code version}.
     */
    ValueUtils.ValueResult lockRead() {
        return ValueUtils.ValueResult.TRUE;
    }

    /**
     * Releases a read lock.
     * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
     *
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockRead() {
        return ValueUtils.ValueResult.TRUE;
    }

    /**
     * Acquires a write lock.
     * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
     *
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult lockWrite() {
        return ValueUtils.ValueResult.TRUE;
    }

    /**
     * Releases a write lock.
     * Sequential Slice doesn't support synchronization, therefore for this type of slice this is NOP.
     *
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockWrite() {
        return ValueUtils.ValueResult.TRUE;
    }

    /**
     * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
     * Expand-only Slice doesn't support deletion, therefore for this type of slice this is NOP.
     *
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult logicalDelete() {
        return ValueUtils.ValueResult.TRUE;
    }

    /**
     * Is the associated off-heap cut marked as logically deleted
     *
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult isDeleted() {
        return ValueUtils.ValueResult.FALSE;
    }

    /**
     * Marks the header of the associated off-heap cut as moved, just write (without CAS)
     * The write lock must be held (asserted inside the header)
     *
     * Expand-only Slice doesn't support move (for now), therefore for this type of slice this is NOP.
     */
    void markAsMoved() { }

    /**
     * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
     * The write lock must be held (asserted inside the header).
     * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
     *
     * Expand-only Slice doesn't support deletion, therefore for this type of slice this is NOP.
     */
    void markAsDeleted() { }

}
