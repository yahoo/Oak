package com.oath.oak;

public interface OakDetachedBuffer extends OakReadBuffer {

    /**
     * Perform a transformation on the inner ByteBuffer atomically.
     *
     * @param transformer The function to apply on the ByteBuffer
     * @return The return value of the transform
     */
    <T> T transform(OakTransformer<T> transformer);

}
