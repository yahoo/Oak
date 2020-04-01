/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ReadOnlyBufferException;

/**
 * A similar to ByteBuffer interface that allows internal Oak data access for read and write
 */
public interface OakWriteBuffer extends OakReadBuffer {

    /**
     * Absolute <i>put</i> method&nbsp;&nbsp;<i>(optional operation)</i>.
     * <p> Writes the given byte into this buffer at the given
     * index. </p>
     *
     * @param index The index at which the byte will be written
     * @param b     The byte value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit
     * @throws ReadOnlyBufferException   If this buffer is read-only
     */
    OakWriteBuffer put(int index, byte b);

    /**
     * Absolute <i>put</i> method for writing a char
     * value&nbsp;&nbsp;<i>(optional operation)</i>.
     * <p> Writes two bytes containing the given char value, in the
     * current byte order, into this buffer at the given index.  </p>
     *
     * @param index The index at which the bytes will be written
     * @param value The char value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     * @throws ReadOnlyBufferException   If this buffer is read-only
     */
    OakWriteBuffer putChar(int index, char value);

    /**
     * Absolute <i>put</i> method for writing a short
     * value&nbsp;&nbsp;<i>(optional operation)</i>.
     * <p> Writes two bytes containing the given short value, in the
     * current byte order, into this buffer at the given index.  </p>
     *
     * @param index The index at which the bytes will be written
     * @param value The short value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     * @throws ReadOnlyBufferException   If this buffer is read-only
     */
    OakWriteBuffer putShort(int index, short value);

    /**
     * Absolute <i>put</i> method for writing an int
     * value&nbsp;&nbsp;<i>(optional operation)</i>.
     * <p> Writes four bytes containing the given int value, in the
     * current byte order, into this buffer at the given index.  </p>
     *
     * @param index The index at which the bytes will be written
     * @param value The int value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     * @throws ReadOnlyBufferException   If this buffer is read-only
     */
    OakWriteBuffer putInt(int index, int value);

    /**
     * Absolute <i>put</i> method for writing a long
     * value&nbsp;&nbsp;<i>(optional operation)</i>.
     * <p> Writes eight bytes containing the given long value, in the
     * current byte order, into this buffer at the given index.  </p>
     *
     * @param index The index at which the bytes will be written
     * @param value The long value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     * @throws ReadOnlyBufferException   If this buffer is read-only
     */
    OakWriteBuffer putLong(int index, long value);

    /**
     * Absolute <i>put</i> method for writing a float
     * value&nbsp;&nbsp;<i>(optional operation)</i>.
     * <p> Writes four bytes containing the given float value, in the
     * current byte order, into this buffer at the given index.  </p>
     *
     * @param index The index at which the bytes will be written
     * @param value The float value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     * @throws ReadOnlyBufferException   If this buffer is read-only
     */
    OakWriteBuffer putFloat(int index, float value);


    /**
     * Absolute <i>put</i> method for writing a double
     * value&nbsp;&nbsp;<i>(optional operation)</i>.
     * <p> Writes eight bytes containing the given double value, in the
     * current byte order, into this buffer at the given index.  </p>
     *
     * @param index The index at which the bytes will be written
     * @param value The double value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     * @throws ReadOnlyBufferException   If this buffer is read-only
     */
    OakWriteBuffer putDouble(int index, double value);

}
