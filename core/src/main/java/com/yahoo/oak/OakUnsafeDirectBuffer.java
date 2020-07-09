/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;

/**
 * This interface allows high performance access to the underlying data of Oak.
 * To achieve that, it sacrifices safety, so it should be used only if you know what you are doing.
 * Misuse of this interface might result in corrupted data, a crash or a deadlock.
 * <p>
 * Specifically, the developer should be concerned by two issues:
 * 1. Concurrency: using this interface inside the context of serialize, compute, compare and transform is thread safe.
 *    In other contexts (e.g., get output), the developer should ensure that there is no concurrent access to this
 *    data. Failing to ensure that might result in corrupted data.
 * 2. Data boundaries: when using this interface, Oak will not alert the developer regarding any out of boundary access.
 *    Thus, the developer should use getOffset() and getLength() to obtain the data boundaries and carefully access
 *    the data. Writing data out of these boundaries might result in corrupted data, a crash or a deadlock.
 * <p>
 * To use this interface, the developer should cast Oak's buffer (OakScopedReadBuffer or OakScopedWriteBuffer) to this
 * interface, similarly to how Java's internal DirectBuffer is used. For example:
 * <pre>
 * {@code
 * int foo(OakScopedReadBuffer b) {
 *     OakUnsafeDirectBuffer ub = (OakUnsafeDirectBuffer) b;
 *     ByteBuffer bb = ub.getByteBuffer();
 *     return bb.getInt(ub.getOffset());
 * }
 * }
 * </pre>
 */
public interface OakUnsafeDirectBuffer {

    /**
     * Allows access to the underlying ByteBuffer of Oak.
     * This buffer might contain data that is unrelated to the context in which this object was introduced.
     * For example, it might contain internal Oak data and other user data.
     * Thus, the developer should use getOffset() and getLength() to validate the data boundaries.
     * Note 1: depending on the context (casting from OakScopedReadBuffer or OakScopedWriteBuffer), the buffer mode
     *         might be ready only.
     * Note 2: the buffer internal state (e.g., byte order, position, limit and so on) should not be modified as this
     *         object might be shared and used elsewhere.
     *
     * @return the underlying ByteBuffer.
     */
    ByteBuffer getByteBuffer();

    /**
     * @return the data offset inside the underlying ByteBuffer.
     */
    int getOffset();

    /**
     * @return the data length.
     */
    int getLength();

    /**
     * Allows access to the memory address of the underlying off-heap ByteBuffer of Oak.
     * The address will point to the beginning of the user data, but avoiding overflow is the developer responsibility.
     * Thus, the developer should use getLength() and access data only in this boundary.
     * This is equivalent to ((DirectBuffer) b.getByteBuffer()).address() + b.getOffset()
     *
     * @return the exact memory address of the underlying buffer in the position of the data.
     */
    long getAddress();

}
