/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

// SliceSyncRecycle represents an data about an off-heap cut: a portion of a bigger block,
// which is part of the underlying (recycling and synchronized) managed off-heap memory.
// SliceSyncRecycle is allocated only via SyncRecycleMemoryManager, and can be de-allocated later.
// Any slice can be either empty or associated with an off-heap cut,
// which is the aforementioned portion of an off-heap memory.
class SliceSyncRecycle extends Slice {
    static final int UNDEFINED_LENGTH_OR_OFFSET = -1;

    /**
     * An allocated by SyncRecycleMemoryManager off-heap cut have reserved space for meta-data, i.e., a header.
     * The header size is defined externally by memory-manager at the slice construction.
     */
    private final int headerSize;
    private final SyncRecycleMMHeader header;
    private int version;    // Allocation time version

    /* ------------------------------------------------------------------------------------
     * Constructors
     * ------------------------------------------------------------------------------------*/
    // Should be used only by Memory Manager (within Memory Manager package)
    SliceSyncRecycle(int headerSize, SyncRecycleMMHeader header) {
        super();
        this.headerSize = headerSize;
        this.header = header;
        invalidate();
    }

    // Should be used only for testing
    @VisibleForTesting SliceSyncRecycle() {
        this(0, null);
    }

    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    SliceSyncRecycle getDuplicatedSlice() {
        SliceSyncRecycle newSlice = new SliceSyncRecycle(this.headerSize, this.header);
        newSlice.copyFrom(this);
        return newSlice;
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/
    // Reset all not final fields to invalid state
    void invalidate() {
        blockID     = NativeMemoryAllocator.INVALID_BLOCK_ID;
        reference   = ReferenceCodecSyncRecycle.INVALID_REFERENCE;
        version     = ReferenceCodecSyncRecycle.INVALID_VERSION;
        length      = UNDEFINED_LENGTH_OR_OFFSET;
        offset      = UNDEFINED_LENGTH_OR_OFFSET;
        memAddress  = UNDEFINED_LENGTH_OR_OFFSET_OR_ADDRESS;
        associated  = false;
    }

    /*
     * Updates everything that can be extracted with reference decoding of "SyncRecycle" type,
     * including reference itself. The separation by reference decoding type is temporary!
     * This is not the full setting of the association, therefore 'associated' flag remains false
     */
    void associateReferenceDecoding(int blockID, int offset, int version, long reference) {
        setBlockIdOffsetAndLength(blockID, offset, UNDEFINED_LENGTH_OR_OFFSET);
        this.reference = reference;
        this.version = version;
        associated   = false;
    }

    // Copy the block allocation information from another block allocation.
    <T extends Slice> void copyFrom(T other) {
        if (other instanceof SliceSyncRecycle) { //TODO: any other ideas?
            copyAllocationInfoFrom(other);
            this.version = ((SliceSyncRecycle) other).version;
            // if SliceSeqExpand gets new members (not included in allocation info)
            // their copy needs to be added here
        } else {
            throw new IllegalStateException("Must provide SliceSyncRecycle other Slice");
        }
    }

    /*
     * Upon allocation, sets everything related to Synchronizing-Recycling memory management of
     * an off-heap cut: a portion of a bigger block.
     * Used only within Memory Manager package.
     */
    void associateMMAllocation(int version, long reference) {
        this.version = version;
        this.reference = reference;
    }

    void updateOnSameBlock(int offset, int length) {
        throw new IllegalStateException("SliceSyncRecycle cannot be partially updated");
    }

    // the method has no effect if length is already set
    protected void prefetchDataLength() {
        if (length == UNDEFINED_LENGTH_OR_OFFSET) {
            // the length kept in header is the length of the data only!
            // add header size
            this.length = header.getDataLength(getMetadataAddress()) + headerSize;
        }
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/
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

    @Override
    public int getLength() {
        // prefetchDataLength() prefetches the length from header only if Slice's length is undefined
        prefetchDataLength();
        return length - headerSize;
    }

    @Override
    public long getAddress() {
        return memAddress + offset + headerSize;
    }

    @Override
    public String toString() {
        return String.format("SliceSyncRecycle(blockID=%d, offset=%,d, length=%,d, version=%d)",
            blockID, offset, length, version);
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
