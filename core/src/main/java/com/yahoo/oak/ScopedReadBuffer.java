/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;

/**
 * An instance of this buffer is only used when the read lock of the key/value referenced by it is already acquired.
 * This is the reason no lock is acquired in each access.
 */
class ScopedReadBuffer implements OakScopedReadBuffer, OakUnsafeDirectBuffer {

    protected final Slice s; // the slice responsible for the off-heap memory for this buffer

    ScopedReadBuffer(Slice s) {
        this.s = s;
    }

    ScopedReadBuffer(ScopedReadBuffer other) {
        this.s = other.s.duplicate();
    }

    protected long getDataAddress(int index) {
        assert isAssociated();
        if (index < 0 || index >= getLength()) {
            throw new IndexOutOfBoundsException(String.format("Index %s is out of bound (length: %s)",
                    index, getLength()));
        }
        return s.getAddress() + index;
    }

    protected void invalidate() {
        s.invalidate();
    }

    protected boolean isAssociated() {
        return s.isAssociated();
    }

    protected Slice getSlice() {
        return s;
    }

    /** ------------------------------ OakScopedReadBuffer ------------------------------ **/
    
    @Override
    public int capacity() {
        return s.getLength();
    }

    @Override
    public byte get(int index) {
        return DirectUtils.get(getDataAddress(index));
    }

    @Override
    public char getChar(int index) {
        return DirectUtils.getChar(getDataAddress(index));
    }

    @Override
    public short getShort(int index) {
        return DirectUtils.getShort(getDataAddress(index));
    }

    @Override
    public int getInt(int index) {
        return DirectUtils.getInt(getDataAddress(index));
    }

    @Override
    public long getLong(int index) {
        return DirectUtils.getLong(getDataAddress(index));
    }

    @Override
    public float getFloat(int index) {
        return DirectUtils.getFloat(getDataAddress(index));
    }

    @Override
    public double getDouble(int index) {
        return DirectUtils.getDouble(getDataAddress(index));
    }

    /** ------------------------------ OakUnsafeDirectBuffer ------------------------------ **/
    
    /**
     * Allows access to the underlying ByteBuffer of Oak.
     * This buffer might contain data that is unrelated to the context in which this object was introduced.
     * For example, it might contain internal Oak data and other user data.
     * Thus, the developer should use getOffset() and getLength() to validate the data boundaries.
     * Note 1: depending on the context (casting from OakScopedReadBuffer or OakScopedWriteBuffer), the buffer mode
     * might be ready only.
     * Note 2: the buffer internal state (e.g., byte order, position, limit and so on) should not be modified as this
     * object might be shared and used elsewhere.
     *
     * @return the underlying ByteBuffer.
     */
    @Override public ByteBuffer getByteBuffer() { 
        return DirectUtils.wrapAddress(s.getAddress(), capacity());
    }

    /**
     * @return the data length.
     */
    @Override public int getLength() {
        assert s.isAssociated();
        return s.getLength();
    }

    /**
     * Allows access to the memory address of the OakUnsafeDirectBuffer of Oak.
     * The address will point to the beginning of the user data, but avoiding overflow is the developer responsibility.
     * Thus, the developer should use getLength() and access data only in this boundary.
     *
     * @return the exact memory address of the Buffer in the position of the data.
     */
    @Override public long getAddress() {
        return s.getAddress();
    }
}
