/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/* A Slice represents an data about an off-heap cut: a portion of a bigger block,
** which is part of the underlying managed off-heap memory. Concrete implementation adapts
** the Slice interface to a specific memory manager.
** Slice is allocated only via memory manager, and can be de-allocated later.
** Slice can be either empty or associated with an off-heap cut,
** which is the aforementioned portion of an off-heap memory.
** */
interface Slice {

    /**
     * Allocate new off-heap cut and associate this slice with a new off-heap cut of memory
     *
     * @param size     the number of bytes required by the user
     * @param existing whether the allocation is for existing off-heap cut moving to the other
     *                 location (e.g. in order to be enlarged).
     */
    void allocate(int size, boolean existing);

    /**
     * Release the associated off-heap cut, which must be disconnected from the data structure,
     * but can still be accessed via threads previously having the access. It is the memory
     * manager responsibility to care for the old concurrent accesses.
     */
    void release();

    /**
     * Reset all Slice fields to invalid value, erase the previous association if existed.
     * This does not releases the associated off-heap cut to memory manager, just disconnects
     * the association!
     */
    void invalidate();

    /* ------------------------------------------------------------------------------------
     * Reference is a long (64 bits) that should encapsulate all the information required
     * to access a memory for read and for write (when Slice is not kept).
     * It is up to memory manager what to put inside.
     * Reference is kept inside the Slice, which is associated to an off-heap cut.
     * ------------------------------------------------------------------------------------*/

    /**
     * Decode information from reference to this Slice's fields.
     *
     * @param reference the reference to decode
     * @return true if the given allocation reference is valid and not deleted. If reference is
     * invalid, the slice is invalidated. If reference is deleted, this slice is updated anyway.
     */
    boolean decodeReference(long reference);

    /* ------------------------------------------------------------------------------------
     * Slices duplication and info transfer
     * ------------------------------------------------------------------------------------*/

    /**
     * Used to duplicate the allocation state. Does not duplicate the underlying off-heap cut itself.
     * Should be used when ThreadContext's internal Slice needs to be exported to the user.
     */
    Slice duplicate();

    /**
     * Copy the off-heap cut allocation information from another off-heap cut allocation.
     */
    void copyFrom(Slice other);

    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    /**
     * Is the Slice associated with a valid off-heap cut of memory?
     */
    boolean isAssociated();

    /**
     * Return the reference which is a long that encapsulates needed to access this off-heap cut
     * later, using decodeReference() method.
     * The reference is valid only for associated with off-heap cut Slice.
     */
    long getReference();

    /**
     * Returns the length of the associated off-heap cut. If any metadata needs to be added to the
     * off-heap cut this metadata length is not included in the answer.
     */
    int getLength();

    /**
     * Allows access to the memory address of the underlying off-heap cut.
     * @return the exact memory address of the off-heap cut in the position of the user data.
     */
    long getAddress();

    /* ------------------------------------------------------------------------------------
     * Off-heap metadata based operations: locking and logical delete
     * ------------------------------------------------------------------------------------*/

    /**
     * Acquires a read lock
     *
     * @return {@code TRUE} if the read lock was acquires successfully
     * {@code FALSE} if the header/off-heap-cut is marked as deleted
     * {@code RETRY} if the header/off-heap-cut was moved, or the version of the off-heap header
     * does not match {@code version}.
     */
    ValueUtils.ValueResult preRead();

    /**
     * Releases a read lock
     *
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult postRead() throws DeletedMemoryAccessException;

    /**
     * Acquires a write lock
     *
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult preWrite();

    /**
     * Releases a write lock
     *
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueUtils.ValueResult postWrite();

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
