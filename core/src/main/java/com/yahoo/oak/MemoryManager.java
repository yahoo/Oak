/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.Closeable;

interface MemoryManager extends Closeable {

    /**
     * This enum indicates whether the slice allocated will belong to a key or to a value.
     */
    enum Allocate {
        KEY, VALUE;
    }

    boolean isClosed();

    /**
     * @return the number of bytes allocated by the {@code allocate} method.
     */
    long allocated();

    /**
     * This method allocates memory out of the off-heap, i.e., the ByteBuffer inside of {@code s} is pointing to the
     * off-heap. The blockID, is an internal reference to which block the ByteBuffer points, allowing the functions
     * {@code getSliceFromBlockID} and {@code getByteBufferFromBlockID} to reconstruct the same ByteBuffer.
     *
     * @param s        - an allocation object to update with the new allocation
     * @param size     - the size of the Slice to allocate
     * @param allocate - whether this Slice is for a key or a value
     */
    void allocate(Slice s, int size, Allocate allocate);

    /**
     * When returning an allocated Slice to the Memory Manager, depending on the implementation, there might be a
     * restriction on whether this allocation is reachable by other threads or not.
     *
     * @param s the allocation object to release
     */
    void release(Slice s);

    /**
     * Fetch the buffer for an allocation that is already set with its parameters: blockID, offset and length.
     */
    void readByteBuffer(Slice s, int blockID);

    /**
     * Version is responsibility of the Memory Manager, but version shouldn't be exposed outside.
     * This is only for testing!!
     * @return the current version
     */
    int getCurrentVersion();

    /**
     * Get ReferenceCodec to manage the (long) references,
     * in which all the info for the memory access is incorporated
     *
     */
    ReferenceCodec getReferenceCodec();

}
