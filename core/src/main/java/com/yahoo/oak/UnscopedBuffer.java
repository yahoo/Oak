/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This is a generic class for key/value un-scoped buffers.
 * This class is used for when an un-scoped access to the key/value is needed without synchronization:
 *  - KeyIterator
 *  - EntryIterator (for keys)
 *  - KeyStreamIterator
 *  - ValueStreamIterator
 *  - EntryStreamIterator (for both keys and values)
 * <p>
 * It should only be used without other concurrent writes in the background to this buffer.
 * <p>
 * A child class that require synchronization, needs to override the following methods
 *  - public <T> T transform(OakTransformer<T> transformer)
 *  - protected <R> R safeAccessToScopedBuffer(Getter<R> getter, int index)
 * See the methods documentation below for more information.
 */
class UnscopedBuffer<B extends ScopedReadBuffer> implements OakUnscopedBuffer, OakUnsafeDirectBuffer {

    final B buffer;

    UnscopedBuffer(B buffer) {
        this.buffer = buffer;
    }

    @Override
    public int capacity() {
        return buffer.capacity();
    }

    @Override
    public ByteOrder order() {
        return buffer.order();
    }

    @Override
    public byte get(int index) {
        return safeAccessToScopedBuffer(ScopedReadBuffer::get, index);
    }

    @Override
    public char getChar(int index) {
        return safeAccessToScopedBuffer(ScopedReadBuffer::getChar, index);
    }

    @Override
    public short getShort(int index) {
        return safeAccessToScopedBuffer(ScopedReadBuffer::getShort, index);
    }

    @Override
    public int getInt(int index) {
        return safeAccessToScopedBuffer(ScopedReadBuffer::getInt, index);
    }

    @Override
    public long getLong(int index) {
        return safeAccessToScopedBuffer(ScopedReadBuffer::getLong, index);
    }

    @Override
    public float getFloat(int index) {
        return safeAccessToScopedBuffer(ScopedReadBuffer::getFloat, index);
    }

    @Override
    public double getDouble(int index) {
        return safeAccessToScopedBuffer(ScopedReadBuffer::getDouble, index);
    }

    /**
     * Returns a transformation of ByteBuffer content.
     * If the child class needs synchronization before accessing the data, it should implement the synchronization
     * in this method, surrounding the call for the transformer function.
     *
     * @param transformer the function that executes the transformation
     * @return a transformation of the ByteBuffer content
     * @throws NullPointerException if the transformer is null
     */
    public <T> T transform(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }
        return transformer.apply(buffer);
    }

    @FunctionalInterface
    public interface Getter<R> {
        R get(ScopedReadBuffer buffer, int index);
    }

    /**
     * Apply a get operation on the inner scoped buffer in a safe manner (atomically if needed).
     * It is used internally by this class for when we use the internal buffer to read the data.
     * If the child class needs synchronization before accessing the data, it should implement the synchronization
     * in this method, surrounding the call for the getter function.
     */
    protected <R> R safeAccessToScopedBuffer(Getter<R> getter, int index) {
        // Internal call. No input validation.
        return getter.get(buffer, index);
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        return buffer.getByteBuffer();
    }

    @Override
    public int getOffset() {
        return buffer.getOffset();
    }

    @Override
    public int getLength() {
        return buffer.getLength();
    }

    @Override
    public long getAddress() {
        return buffer.getAddress();
    }
}
