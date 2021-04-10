/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

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
    private final int headerSize;
    private final SyncRecycleMMHeader header;

    /** The fields describing the associated off-heap cut, they are set when slice is not empty **/
    private long reference;
    private final long invalidReferenceValue; // used for invalidation
    private int blockID = NativeMemoryAllocator.INVALID_BLOCK_ID;
    private int offset  = UNDEFINED_LENGTH_OR_OFFSET;

    // The entire length of the off-heap cut, including the header!
    private int length = UNDEFINED_LENGTH_OR_OFFSET;
    private int version;    // Allocation time version
    private long memAddress= 0;
    // true if slice is associated with an off-heap slice of memory
    // if associated is false the Slice is empty
    private boolean associated = false;

    /* ------------------------------------------------------------------------------------
     * Constructors
     * ------------------------------------------------------------------------------------*/
    // Should be used only by Memory Manager (within Memory Manager package)
    Slice(int headerSize, long invalidReferenceValue, SyncRecycleMMHeader header) {
        this.headerSize = headerSize;
        this.invalidReferenceValue = invalidReferenceValue;
        this.header = header;
        invalidate();
    }

    // Should be used only for testing
    @VisibleForTesting
    Slice() {
        this(0, 0, null);
    }

    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    Slice getDuplicatedSlice() {
        Slice newSlice = new Slice(this.headerSize, this.invalidReferenceValue, this.header);
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
        memAddress      = 0;
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
    void associateBlockAllocation(int blockID, int offset, int length, long memAddress){
        assert blockID != NativeMemoryAllocator.INVALID_BLOCK_ID
            && offset > UNDEFINED_LENGTH_OR_OFFSET && length > UNDEFINED_LENGTH_OR_OFFSET
            && memAddress != 0;
        setBlockIdOffsetAndLength(blockID, offset, length);
        this.memAddress  = memAddress;
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
        this.memAddress = other.memAddress;
        this.reference = other.reference;
        this.associated = other.associated;
    }

    // Set the internal buffer.
    // This method should be used only within Memory Management package.
    void setAddress(long memAddress) {
        this.memAddress= memAddress;
        assert memAddress != 0;
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
    void updateOnSameBlock(int offset, int length) {
        this.offset = offset;
        this.length = length;
        assert memAddress != 0;
        this.associated = true;
    }

    // the method has no effect if length is already set
    protected void prefetchDataLength() {
        if (length == UNDEFINED_LENGTH_OR_OFFSET) {
            // the length kept in header is the length of the data only!
            // add header size
            this.length = header.getDataLength(getMetadataAddress()) + headerSize;
        }
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
        // prefetchDataLength() prefetches the length from header only if Slice's length is undefined
        prefetchDataLength();
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
        return memAddress+ offset;
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/  

    @Override
    public int getOffset() {
        assert associated;
        return offset + headerSize;
    }

    @Override
    public int getLength() {
        // prefetchDataLength() prefetches the length from header only if Slice's length is undefined
        prefetchDataLength();
        return length - headerSize;
    }

    @Override
    public long getAddress() {
        return memAddress + getOffset();
    }

    @Override
    public String toString() {
        return String.format("Slice(blockID=%d, offset=%,d, length=%,d, version=%d)", blockID, offset, length, version);
    }
    
    //must never be called internally kept in here to beautify the interface
    @Override
    public ByteBuffer getByteBuffer() { 
        throw new RuntimeException(); 
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
        assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
        return header.lockRead(version, getMetadataAddress());
    }

    /**
     * Releases a read lock
     *
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockRead(){
        assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
        return header.unlockRead(version, getMetadataAddress());
    }

    /**
     * Acquires a write lock
     *
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult lockWrite(){
        assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
        return header.lockWrite(version, getMetadataAddress());
    }

    /**
     * Releases a write lock
     *
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockWrite(){
        assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
        return header.unlockWrite(version, getMetadataAddress());
    }

    /**
     * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
     *
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult logicalDelete(){
        assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
        return header.logicalDelete(version, getMetadataAddress());
    }

    /**
     * Is the associated off-heap cut marked as logically deleted
     *
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult isDeleted(){
        assert version != ReferenceCodecSyncRecycle.INVALID_VERSION;
        return header.isLogicallyDeleted(version, getMetadataAddress());
    }

    /**
     * Marks the header of the associated off-heap cut as moved, just write (without CAS)
     * The write lock must be held (asserted inside the header)
     */
    void markAsMoved() {
        assert associated;
        header.markAsMoved(getMetadataAddress());
    }

    /**
     * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
     * The write lock must be held (asserted inside the header).
     * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
     */
    void markAsDeleted() {
        assert associated;
        header.markAsDeleted(getMetadataAddress());
    }
    
}
