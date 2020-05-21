/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

/**
 * This buffer may represent either a key or a value for read/write access.
 * It mimic the standard interface of Java's ByteBuffer, for example, int getInt(int index), char getChar(int index),
 * capacity(), etc.
 *
 * It is attached to the scope of the callback method it were first introduced to the user.
 * The behaviour of this buffer outside their attached scope is undefined.
 * Such callback method might be the application's serializer and comparator, or a lambda function that can
 * read/store/update the data.
 * This access reduces unnecessary copies and deserialization of the underlying data.
 * In their intended context, the user does not need to worry about concurrent accesses and memory management.
 * Using these buffers outside their intended context may yield unpredicted results, e.g., reading non-consistent data
 * and/or irrelevant data.
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
     */
    OakWriteBuffer putDouble(int index, double value);

}
