/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
        this.s = other.s.getDuplicatedSlice();
    }

    protected long getDataOffset(int index) {
        if (index < 0 || index >= getLength()) {
            throw new IndexOutOfBoundsException(String.format("Index %s is out of bound (length: %s)",
                    index, getLength()));
        }
        return s.getAddress() + index;
    }

    protected void invalidate() {
        s.invalidate();
    }

    protected boolean isInitiated() {
        return s.isInitiated();
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
    public ByteOrder order() {
        return s.getByteBuffer().order();
    }

    @Override
    public byte get(int index) {
        return UnsafeUtils.unsafe.getByte(getDataOffset(index));
    }

    @Override
    public char getChar(int index) {
        return UnsafeUtils.unsafe.getChar(getDataOffset(index));
    }

    @Override
    public short getShort(int index) {
        return UnsafeUtils.unsafe.getShort(getDataOffset(index));
    }

    @Override
    public int getInt(int index) {
        return UnsafeUtils.unsafe.getInt(getDataOffset(index));
    }

    @Override
    public long getLong(int index) {
        return UnsafeUtils.unsafe.getLong(getDataOffset(index));
    }

    @Override
    public float getFloat(int index) {
        return UnsafeUtils.unsafe.getFloat(getDataOffset(index));
    }

    @Override
    public double getDouble(int index) {
        return UnsafeUtils.unsafe.getDouble(getDataOffset(index));
    }

    /** ------------------------------ OakUnsafeDirectBuffer ------------------------------ **/
    
    /**
     * @return the data offset inside the underlying ByteBuffer.
     */
    @Override public int getOffset() {
        return s.getOffset();
    }

    /**
     * @return the data length.
     */
    @Override public int getLength() {
        return s.getLength();
    }

    /**
     * Allows access to the memory address of the OakUnsafeDirectBuffer of Oak.
     * The address will point to the beginning of the user data, but avoiding overflow is the developer responsibility.
     * Thus, the developer should use getLength() and access data only in this boundary.
     *
     * @return the exact memory address of the Buffer in the position of the data.
     */
    @Override public long getAddress() { //for both OakUnsafeDirectBuffer -- OakScopedReadBuffer
        return s.getAddress();
    }
    
   //must never be called internally kept in here to beautify the interface
    @Override public ByteBuffer getByteBuffer() { 
        throw new RuntimeException(); 
    }
    //used for wrapping address with bytebuffer for external use
    public int getNativeCapacity() {
        return s.getCapacity();
    }
    
}
