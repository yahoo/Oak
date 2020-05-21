/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteOrder;

/**
 * This buffer may represent either a key or a value for read access.
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
public interface OakReadBuffer {

    /**
     * Returns this buffer's capacity.
     *
     * @return The capacity of this buffer
     */
    int capacity();

    /**
     * Absolute <i>get</i> method.  Reads the byte at the given
     * index.
     *
     * @param index The index from which the byte will be read
     * @return The byte at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit
     */
    byte get(int index);

    /**
     * Retrieves this buffer's byte order.
     * <p> The byte order is used when reading or writing multibyte values, and
     * when creating buffers that are views of this byte buffer.  The order of
     * a newly-created byte buffer is always {@link ByteOrder#BIG_ENDIAN
     * BIG_ENDIAN}.  </p>
     *
     * @return This buffer's byte order
     */
    ByteOrder order() throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a char value.
     * <p> Reads two bytes at the given index, composing them into a
     * char value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The char value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     */
    char getChar(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a short value.
     * <p> Reads two bytes at the given index, composing them into a
     * short value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The short value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     */
    short getShort(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading an int value.
     * <p> Reads four bytes at the given index, composing them into a
     * int value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The int value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     */
    int getInt(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a long value.
     * <p> Reads eight bytes at the given index, composing them into a
     * long value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The long value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     */
    long getLong(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a float value.
     * <p> Reads four bytes at the given index, composing them into a
     * float value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The float value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     */
    float getFloat(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a double value.
     * <p> Reads eight bytes at the given index, composing them into a
     * double value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The double value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     */
    double getDouble(int index) throws NullPointerException;
}
