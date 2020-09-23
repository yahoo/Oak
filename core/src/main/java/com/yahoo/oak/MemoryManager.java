/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.Closeable;

/*** Memory Manager should efficiently provide:
 ** (1) Memory allocation services
 ** (2) Memory reclamation services
 ** (3) Safe access, in a way that a reused memory allocated to a new usage can be never accessed
 ** via old references to the same location.*/
interface MemoryManager extends Closeable {

    boolean isClosed();

    /**
     * @return the number of bytes allocated by the {@code allocate} method.
     */
    long allocated();

    /**
     * This method allocates memory out of the off-heap, i.e., the ByteBuffer inside of {@code s} is pointing to the
     * off-heap. The blockID, is an internal reference to which block the ByteBuffer points, allowing the functions
     * {@code getSliceFromBlockID} and {@code getByteBufferFromBlockID} to reconstruct the same ByteBuffer.
     *  @param s        - an allocation object to update with the new allocation
     * @param size     - the size of the Slice to allocate
     */
    void allocate(Slice s, int size);

    /**
     * When returning an allocated Slice to the Memory Manager, depending on the implementation, there might be a
     * restriction on whether this allocation is reachable by other threads or not.
     *
     * @param s the allocation object to release
     */
    void release(Slice s);

    /* ------------- Interfaces to deal with references! ------------- */
    /* Reference is a long (64 bits) that should encapsulate all the information required
    * to access a memory for read and for write. It is up to memory manager what to put inside.
    */

    /**
     * @param s         the memory slice to update with the info decoded from the reference
     * @param reference the reference to decode
     * @return true if the given allocation reference is valid and not deleted. If reference is
     * invalid, the slice is invalidated. If reference is deleted, the slice is updated anyway.
     */
    boolean decodeReference(Slice s, long reference);

    /**
     * @param s the memory slice, encoding of which should be returned as a an output long reference
     * @return the encoded reference
     */
    long encodeReference(Slice s);

    /** Present the reference as it needs to be when the target is deleted
     * @param reference to alter
     * @return the encoded reference
     */
    long alterReferenceForDelete(long reference);

    /** Provide reference considered invalid (null) by this memory manager */
    long getInvalidReference();

    // check if reference is valid, according to the reference coding implementation
    boolean isReferenceValid(long reference);

    // check if reference is deleted, according to the reference coding implementation
    boolean isReferenceDeleted(long reference);

    // check if reference is valid and not deleted, in one function call
    // according to the reference codec implementation
    boolean isReferenceValidAndNotDeleted(long reference);

    // invoked (only within assert statement) to check
    // the consistency and correctness of the reference encoding
    boolean isReferenceConsistent(long reference);

    // invoked to get the slice (implementing Slice interface)
    // Slice may have different implementation for different Memory Managers
    Slice getEmptySlice();

}
