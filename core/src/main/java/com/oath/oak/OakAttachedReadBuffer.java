/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class OakAttachedReadBuffer implements OakReadBuffer, OakUnsafeDirectBuffer {

    protected ByteBuffer bb;
    protected int dataPos;
    protected int dataLength;

    OakAttachedReadBuffer(Slice s, int headerSize) {
        bb = s.getByteBuffer();
        dataPos = bb.position() + headerSize;
        dataLength = bb.limit() - dataPos;
    }

    protected void checkIndex(int index) {
        if (index < 0 || index >= dataLength) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public int capacity() {
        return dataLength;
    }

    @Override
    public ByteOrder order() {
        return bb.order();
    }

    @Override
    public byte get(int index) {
        checkIndex(index);
        return bb.get(dataPos + index);
    }

    @Override
    public char getChar(int index) {
        checkIndex(index);
        return bb.getChar(dataPos + index);
    }

    @Override
    public short getShort(int index) {
        checkIndex(index);
        return bb.getShort(dataPos + index);
    }

    @Override
    public int getInt(int index) {
        checkIndex(index);
        return bb.getInt(dataPos + index);
    }

    @Override
    public long getLong(int index) {
        checkIndex(index);
        return bb.getLong(dataPos + index);
    }

    @Override
    public float getFloat(int index) {
        checkIndex(index);
        return bb.getFloat(dataPos + index);
    }

    @Override
    public double getDouble(int index) {
        checkIndex(index);
        return bb.getDouble(dataPos + index);
    }

    /*-------------- OakUnsafeRef --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer tempBuff = bb.asReadOnlyBuffer();
        tempBuff.position(dataPos);
        tempBuff.limit(bb.limit());
        return tempBuff.slice();
    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return dataLength;
    }

    @Override
    public long getAddress() {
        long address = ((DirectBuffer) bb).address();
        return address + dataPos;
    }
}
