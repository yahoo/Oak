/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class OakReadBufferWrapper implements OakReadBuffer, OakUnsafeDirectBuffer {

    protected final ByteBuffer buff;
    protected final int offset;
    protected final int length;

    OakReadBufferWrapper(ByteBuffer buff, int headerSize) {
        this(buff, buff.position(), buff.limit(), headerSize);
    }

    OakReadBufferWrapper(Slice s, int headerSize) {
        this(s.getByteBuffer(), s.getPosition(), s.getLimit(), headerSize);
    }

    OakReadBufferWrapper(ByteBuffer buff, int offset, int limit, int headerSize) {
        this.buff = buff;
        this.offset = offset + headerSize;
        this.length = limit - this.offset;
    }

    @Override
    public int capacity() {
        return length;
    }

    protected void checkIndex(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public byte get(int index) {
        checkIndex(index);
        return buff.get(offset + index);
    }

    @Override
    public ByteOrder order() {
        return buff.order();
    }

    @Override
    public char getChar(int index) {
        checkIndex(index);
        return buff.getChar(offset + index);
    }

    @Override
    public short getShort(int index) {
        checkIndex(index);
        return buff.getShort(offset + index);
    }

    @Override
    public int getInt(int index) {
        checkIndex(index);
        return buff.getInt(offset + index);
    }

    @Override
    public long getLong(int index) {
        checkIndex(index);
        return buff.getLong(offset + index);
    }

    @Override
    public float getFloat(int index) {
        checkIndex(index);
        return buff.getFloat(offset + index);
    }

    @Override
    public double getDouble(int index) {
        checkIndex(index);
        return buff.getChar(offset + index);
    }

    /*-------------- OakUnsafeRef --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer tempBuff = buff.asReadOnlyBuffer();
        tempBuff.position(offset);
        tempBuff.limit(offset + length);
        return tempBuff.slice();
    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return capacity();
    }

    @Override
    public long getAddress() {
        long address = ((DirectBuffer) buff).address();
        return address + offset;
    }
}
