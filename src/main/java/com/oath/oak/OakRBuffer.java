/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

/**
 * A similar to read-only ByteBuffer interface that allows internal Oak data read access
 *
 * Pay attention! There is no need to wrap each OakRBuffer interface implementation
 * with attach/detach thread, because OakRKeyBufferImpl is used only within keyIterator, which
 * has attach/detach thread on its own. For the same reason here is no transform() method.
 * On another hand OakRValueBufferImpl has reference to handle which has delete bit and has JVM GC
 * protection.
 */
public interface OakRBuffer {

    /**
     * Returns this buffer's capacity.
     *
     * @return The capacity of this buffer
     */
    public int capacity() throws NullPointerException;

    /**
     * Returns this buffer's position.
     *
     * @return The position of this buffer
     */
    public int position() throws NullPointerException;

    /**
     * Returns this buffer's limit.
     *
     * @return The limit of this buffer
     */
    public int limit() throws NullPointerException;

    /**
     * Returns the number of elements between the current position and the
     * limit.
     *
     * @return The number of elements remaining in this buffer
     */
    public int remaining() throws NullPointerException;

    /**
     * Tells whether there are any elements between the current position and
     * the limit.
     *
     * @return <tt>true</tt> if, and only if, there is at least one element
     * remaining in this buffer
     */
    public boolean hasRemaining() throws NullPointerException;

    /**
     * Absolute <i>get</i> method.  Reads the byte at the given
     * index.
     *
     * @param index The index from which the byte will be read
     * @return The byte at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit
     */
    public byte get(int index) throws NullPointerException;

    /**
     * Retrieves this buffer's byte order.
     * <p>
     * <p> The byte order is used when reading or writing multibyte values, and
     * when creating buffers that are views of this byte buffer.  The order of
     * a newly-created byte buffer is always {@link ByteOrder#BIG_ENDIAN
     * BIG_ENDIAN}.  </p>
     *
     * @return This buffer's byte order
     */
    public ByteOrder order() throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a char value.
     * <p>
     * <p> Reads two bytes at the given index, composing them into a
     * char value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The char value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     */
    public char getChar(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a short value.
     * <p>
     * <p> Reads two bytes at the given index, composing them into a
     * short value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The short value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     */
    public short getShort(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading an int value.
     * <p>
     * <p> Reads four bytes at the given index, composing them into a
     * int value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The int value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     */
    public int getInt(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a long value.
     * <p>
     * <p> Reads eight bytes at the given index, composing them into a
     * long value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The long value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     */
    public long getLong(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a float value.
     * <p>
     * <p> Reads four bytes at the given index, composing them into a
     * float value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The float value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     */
    public float getFloat(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a double value.
     * <p>
     * <p> Reads eight bytes at the given index, composing them into a
     * double value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The double value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     */
    public double getDouble(int index) throws NullPointerException;
}
