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
     * Returns a {@link ByteBuffer} wrapping the underlying memory segment backing this buffer.
     * <p>
     * Note that returned {@link ByteBuffer} might be read-only, if it is obtained from a buffer
     * implementing {@link OakScopedReadBuffer} but not {@link OakScopedWriteBuffer}. Such a buffer is
     * read-only. So should {@link ByteBuffer}s returned from it.
     * <p>
     * Since a new {@link ByteBuffer} instance is returned on each invocation of this method, it's
     * absolutely safe to modify the state of the returned {@link ByteBuffer} including its byte
     * order, position, and limit.
     *
     * <h4>NOTE ON CONCURRENT ACCESSES</h4>
     * Two {@link ByteBuffer}s from the same {@link OakUnsafeDirectBuffer} will reference the same
     * underlying memory, and no coordination is implemented for concurrent accesses via different
     * {@link ByteBuffer} instances. So concurrent accesses with at least one write may result in
     * inconsistent reads and/or render the state of the buffer inconsistent, unless there is proper
     * coordination (synchronization) implemented outside.
     *
     * @return a ByteBuffer wrapping the underlying memory segment
     */
    ByteBuffer getByteBuffer();

    /**
     * @return the data length.
     */
    int getLength();

    /**
     * Allows access to the memory address of the OakUnsafeDirectBuffer of Oak.
     * The address will point to the beginning of the user data, but avoiding overflow is the developer responsibility.
     * Thus, the developer should use getLength() and access data only in this boundary.
     *
     * @return the exact memory address of the Buffer in the position of the data.
     */
    long getAddress();

}
