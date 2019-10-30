/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

// this is the interface to be implemented to replace the OakNativeMemoryAllocator
// this is about allocation of a new Slice (which has inside it a DirectByteBuffer in order to continue supporting
// off-heap memory).
// allocator is also getting a Slice to reuse the memory, given this Slice is no longer in use by any thread.
// Note that Slice cannot be merged into a single Slice, and a Slice currently is not split.

public interface OakBlockMemoryAllocator {

    // Allocates ByteBuffer of the given size, thread safe.
    Slice allocateSlice(int size, MemoryManager.Allocate allocate);

    // Releases ByteBuffer (makes it available for reuse) without other GC consideration.
    // IMPORTANT: it is assumed free will get ByteBuffers only initially allocated from this
    // Allocator!
    void freeSlice(Slice s);

    // Is invoked when entire OakMap is closed
    void close();

    // Returns the memory allocation of this OakMap (this Allocator)
    long allocated();

    // Translates from blockID, buffer position and buffer length to ByteBuffer
    ByteBuffer readByteBufferFromBlockID(int blockID, int bufferPosition, int bufferLength);

    // Check if this Allocator was already closed
    boolean isClosed();
}