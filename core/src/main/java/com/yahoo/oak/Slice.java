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
interface Slice {
    /**
     * Allocate new off-heap cut and associated this slice with a new off-heap cut of memory
     *
     * @param size     the number of bytes required by the user
     * @param existing whether the allocation is for existing off-heap cut moving to the other
     *                 location (e.g. in order to be enlarged).
     */
    void allocate(int size, boolean existing);

    /**
     * Release the associated off-heap cut, which is disconnected from the data structure,
     * but can be still accessed via threads previously having the access. It is the memory
     * manager responsibility to care for the old concurrent accesses.
     */
    void release();

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
    boolean decodeReference(long reference);

    /**
     * Encode (create) the reference according to the information in this Slice
     *
     * @return the encoded reference
     */
    long encodeReference();

    // Reset all common not final fields to invalid state
    void invalidate();

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/
    // Used to duplicate the allocation state. Does not duplicate the underlying memory buffer itself.
    // Should be used when ThreadContext's internal Slice needs to be exported to the user.
    Slice getDuplicatedSlice();

    // Copy the block allocation information from another block allocation.
    <T extends Slice> void copyFrom(T other);

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    boolean isInitiated();

    long getReference();

    int getAllocatedBlockID();

    int getAllocatedOffset();

    int getAllocatedLength();

    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/
    long getMetadataAddress();

    int getLength();

    long getAddress();

    String toString();

    /*-------------- Off-heap header operations: locking and logical delete --------------*/

    /**
     * Acquires a read lock
     *
     * @return {@code TRUE} if the read lock was acquires successfully
     * {@code FALSE} if the header/off-heap-cut is marked as deleted
     * {@code RETRY} if the header/off-heap-cut was moved, or the version of the off-heap header
     * does not match {@code version}.
     */
    ValueUtils.ValueResult lockRead();

    /**
     * Releases a read lock
     *
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockRead();

    /**
     * Acquires a write lock
     *
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult lockWrite();

    /**
     * Releases a write lock
     *
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult unlockWrite();

    /**
     * Marks the associated off-heap cut as deleted only if the version of that value matches {@code version}.
     *
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult logicalDelete();

    /**
     * Is the associated off-heap cut marked as logically deleted
     *
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult isDeleted();

    /**
     * Marks the header of the associated off-heap cut as moved, just write (without CAS)
     * The write lock must be held (asserted inside the header)
     */
    void markAsMoved();

    /**
     * Marks the header of the associated off-heap cut as deleted, just write (without CAS)
     * The write lock must be held (asserted inside the header).
     * It is similar to logicalDelete() but used when locking and marking don't happen in one CAS
     */
    void markAsDeleted();
}
