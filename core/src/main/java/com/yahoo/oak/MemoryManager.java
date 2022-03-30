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
 ** via old references to the same location.
 *
 * Slice is an object that depicts the physical (virtual) memory addresses being references.
 * The memory addresses are named "off-heap cut".
 * */
interface MemoryManager extends Closeable {

    boolean isClosed();

    /**
     * @return the number of bytes allocated by the {@code allocate} method.
     */
    long allocated();

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

    // Returns the BlockMemoryAllocator this memory manager is based on
    // Multiple memory managers may have the same memory allocators
    BlockMemoryAllocator getBlockMemoryAllocator();

    // Used only for testing
    // returns the size of the header used in off-heap to keep Memory Manager's metadata
    @VisibleForTesting
    int getHeaderSize();
    
    @VisibleForTesting
    int getFreeListSize();
    
    @VisibleForTesting
    int getReleaseLimit();

    // Releases the underlying off-heap memory without releasing the entire structure
    // To be used when the user structure needs to be cleared, without memory reallocation
    // NOT THREAD SAFE!!!
    void clear(boolean closeAllocator);

}
