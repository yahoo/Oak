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

    private final SeqExpandMemoryManager semm;  // The memory manager for allocation and release
    private final ReferenceCodecSeqExpand rcse; // The reference codec for references management via Slice

    /* ------------------------------------------------------------------------------------
     * Constructors
     * ------------------------------------------------------------------------------------*/
    // Should be used only by Memory Manager (within Memory Manager package)
    SliceSeqExpand(SeqExpandMemoryManager semm, ReferenceCodecSeqExpand rcse) {
        super();
        this.semm = semm;
        this.rcse = rcse;
        invalidate();
    }

    /**
     * Allocate new off-heap cut and associated this slice with a new off-heap cut of memory
     *
     * @param size     the number of bytes required by the user
     * @param existing whether the allocation is for existing off-heap cut moving to the other
     */
    @Override
    void allocate(int size, boolean existing) {
        reference = semm.allocate(this, size, existing);
    }

    /**
     * Release the associated off-heap cut, which is disconnected from the data structure,
     * but can be still accessed via threads previously having the access. It is the memory
     * manager responsibility to care for the old concurrent accesses.
     */
    @Override
    void release() {
        semm.release(this);
    }

    /**
     * Decode information from reference to this Slice's fields.
     *
     * @param reference the reference to decode
     * @return true if the given allocation reference is valid and not deleted. If reference is
     * invalid, the slice is invalidated. If reference is deleted, this slice is updated anyway.
     */
    @Override
    boolean decodeReference(long reference) {
        return semm.decodeReference(this, reference);
    }

    /**
     * Encode (create) the reference according to the information in this Slice
     *
     * @return the encoded reference
     */
    @Override
    long encodeReference() {
        return rcse.encode(getAllocatedBlockID(), getAllocatedOffset(), getAllocatedLength());
    }

    @Override
    long getThirdForReferenceEncoding() {
        return getAllocatedLength();
    }

    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    SliceSeqExpand getDuplicatedSlice() {
        SliceSeqExpand newSlice = new SliceSeqExpand(this.semm, this.rcse);
        newSlice.copyFrom(this);
        return newSlice;
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/
    // Reset all common not final fields to invalid state
    void invalidate() {
        blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
        reference   = ReferenceCodecSeqExpand.INVALID_REFERENCE;
        length      = undefinedLengthOrOffsetOrAddress;
        offset      = undefinedLengthOrOffsetOrAddress;
        memAddress  = undefinedLengthOrOffsetOrAddress;
        associated  = false;
    }

    /*
     * Updates everything that can be extracted with reference decoding of "SeqExpand" type,
     * including reference itself.
     * This is not the full setting of the association, therefore 'associated' flag remains false
     */
    @Override
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
        assert memAddress != undefinedLengthOrOffsetOrAddress;
        this.associated = true;
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
