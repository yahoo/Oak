/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

// SliceSeqExpand represents an data about an off-heap cut: a portion of a bigger block,
// which is part of the underlying managed off-heap memory, which is managed for sequential
// access and without memory recycling.
// SliceSeqExpand is allocated only via SeqExpandMemoryManager, and can be de-allocated later.
// Any slice can be either empty or associated with an off-heap cut,
// which is the aforementioned portion of an off-heap memory.
class SliceSeqExpand extends Slice {
    static final int UNDEFINED_LENGTH_OR_OFFSET = -1;

    private final long invalidReferenceValue; // used for invalidation

    /* ------------------------------------------------------------------------------------
     * Constructors
     * ------------------------------------------------------------------------------------*/
    // Should be used only by Memory Manager (within Memory Manager package)
    SliceSeqExpand() {
        super();
        this.invalidReferenceValue = ReferenceCodecSeqExpand.getInvalidReference();
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
    // Reset all not final fields to invalid state
    void invalidate() {
        blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
        reference   = invalidReferenceValue;
        length      = UNDEFINED_LENGTH_OR_OFFSET;
        offset      = UNDEFINED_LENGTH_OR_OFFSET;
        buffer      = null;
        associated  = false;
    }

    /*
     * Updates everything that can be extracted with reference decoding of "SeqExpand" type,
     * including reference itself.
     * This is not the full setting of the association, therefore 'associated' flag remains false
     */
    void associateReferenceDecoding(int blockID, int offset, int length, long reference){
        // length can remain undefined until requested, but if given length should include the header
        assert length != UNDEFINED_LENGTH_OR_OFFSET;
        setBlockIdOffsetAndLength(blockID, offset, length);
        this.reference = reference;
        associated   = false;
    }

    // Copy the block allocation information from another block allocation.
    <T extends Slice> void copyFrom(T other){
        if (other instanceof SliceSeqExpand) { //TODO: any other ideas?
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
        } else {
            throw new IllegalStateException("Must provide SliceSeqExpand other Slice");
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
        assert buffer != null;
        this.associated = true;
    }

    // the method has no effect for SliceSeqExpand
    protected void prefetchDataLength() {
        return;
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


    /*-------------- OakUnsafeDirectBuffer --------------*/
    @Override
    public int getOffset() {
        assert associated;
        return offset;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public String toString() {
        return String.format("SliceSeqExpand(blockID=%d, offset=%,d, length=%,d)",
            blockID, offset, length);
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
    ValueUtils.ValueResult lockRead(){
        throw new UnsupportedOperationException("Sequential Slice doesn't support synchronization");
    }

    /**
     * Releases a read lock
     *
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockRead(){
        throw new UnsupportedOperationException("Sequential Slice doesn't support synchronization");
    }

    /**
     * Acquires a write lock
     *
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult lockWrite(){
        throw new UnsupportedOperationException("Sequential Slice doesn't support synchronization");
    }

    /**
     * Releases a write lock
     *
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockWrite(){
        throw new UnsupportedOperationException("Sequential Slice doesn't support synchronization");
    }

    /**
     * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
     *
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult logicalDelete(){
        throw new UnsupportedOperationException("Expand-only Slice doesn't support deletion");
    }

    /**
     * Is the associated off-heap cut marked as logically deleted
     *
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult isDeleted(){
        return ValueUtils.ValueResult.FALSE;
    }

    /**
     * Marks the header of the associated off-heap cut as moved, just write (without CAS)
     * The write lock must be held (asserted inside the header)
     */
    void markAsMoved() {
        throw new UnsupportedOperationException("Expand-only Slice doesn't support moving");
    }

    /**
     * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
     * The write lock must be held (asserted inside the header).
     * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
     */
    void markAsDeleted() {
        throw new UnsupportedOperationException("Expand-only Slice doesn't support deletion");
    }
}
