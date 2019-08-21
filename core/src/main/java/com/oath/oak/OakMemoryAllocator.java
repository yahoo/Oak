/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

// this is the interface to be implemented to replace the OakNativeMemoryAllocator
// this is about allocation of a new ByteBuffer (which need to be DirectByteBuffer
// in order to continue supporting off-heap)
// allocator is also getting a ByteBuffer to reuse the memory, given ByteBuffer is
// no longer in use by any thread
public interface OakMemoryAllocator {

    // Allocates ByteBuffer of the given size, thread safe.
    ByteBuffer allocate(int size);

    // Releases ByteBuffer (makes it available for reuse) without other GC consideration.
    // IMPORTANT: it is assumed free will get ByteBuffers only initially allocated from this
    // Allocator!
    void free(ByteBuffer bb);

    // Is invoked when entire OakMap is closed
    void close();

    // Returns the memory allocation of this OakMap (this Allocator)
    long allocated();
}