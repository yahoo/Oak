package com.oath.oak;

/**
 * As opposed to attached buffers, detached buffer can be returned to the user and may be stored for future use.
 *
 * The zero-copy methods returns this buffer to avoid copying the data and instead the user can access to the
 * underlying memory buffer directly (lazy evaluation).
 * While the attached buffers' data accesses are synchronized, when using OakDetachedBuffer, the same memory might be
 * access by a concurrent update operations.
 * Thus, the reader may encounter different values -- and even value deletions -- when accessing OakDetachedBuffer
 * multiple times.
 * Specifically, when trying to access a deleted mapping via a OakDetachedBuffer, ConcurrentModificationException will
 * be thrown.
 * This is of course normal behavior for a concurrent map that avoids copying.
 * To allow complex, multi-value atomic operations on the data, OakDetachedBuffer provides a transform() method that
 * allow the user to apply a transformation function atomically on a read-only, attached version of the buffer
 * (OakReadBuffer).
 *
 * It is used in zero-copy API for:
 *   (1) get operations
 *   (2) iterations
 *   (3) stream-iterations, where we reuse the same detached buffer, i.e., it refers to different internal
 *       buffers as we iterate the map.
 **/
public interface OakDetachedBuffer extends OakReadBuffer {

    /**
     * Perform a transformation on the inner ByteBuffer atomically.
     *
     * @param transformer The function to apply on the ByteBuffer
     * @return The return value of the transform
     */
    <T> T transform(OakTransformer<T> transformer);

}
