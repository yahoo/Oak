package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.function.Function;

public interface OakUnsafeRef {

    /**
     * @return the underlying ByteBuffer at position 0
     */
    ByteBuffer getByteBuffer();

    /**
     * @return the key/value offset in the underlying ByteBuffer
     */
    int getOffset();

    /**
     * @return the key/value length
     */
    int getLength();

    /**
     * @return the exact address of the underlying buffer in the position of the key/value
     */
    long getAddress();

}
