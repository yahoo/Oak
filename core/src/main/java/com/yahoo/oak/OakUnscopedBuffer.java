/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * As opposed to scoped buffers, un-scoped buffers can be returned to the user and may be stored for future use.
 * <p>
 * The zero-copy methods returns this buffer to avoid copying the data and instead the user can access to the
 * underlying memory buffer directly (lazy evaluation).
 * While the scoped buffers' data accesses are synchronized, when using OakUnscopedBuffer, the same memory might be
 * access by a concurrent update operations.
 * Thus, the reader may encounter different values -- and even value deletions -- when accessing OakUnscopedBuffer
 * multiple times.
 * Specifically, when trying to access a deleted mapping via a OakUnscopedBuffer, ConcurrentModificationException will
 * be thrown.
 * This is of course normal behavior for a concurrent map that avoids copying.
 * To allow complex, multi-value atomic operations on the data, OakUnscopedBuffer provides a transform() method that
 * allow the user to apply a transformation function atomically on a read-only, scoped version of the buffer
 * (OakScopedReadBuffer).
 * <p>
 * It is used in zero-copy API for:
 *   (1) get operations
 *   (2) iterations
 *   (3) stream-iterations, where we reuse the same un-scoped buffer, i.e., it refers to different internal
 *       buffers as we iterate the map.
 **/
public interface OakUnscopedBuffer extends OakBuffer {

    /**
     * Perform a transformation on the inner ByteBuffer atomically.
     *
     * @param transformer The function to apply on the OakScopedReadBuffer
     * @return The return value of the transform
     */
    <T> T transform(OakTransformer<T> transformer);

}
