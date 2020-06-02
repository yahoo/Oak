/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteOrder;

/**
 * An instance of this buffer is only used when the read lock of the key/value referenced by it is already acquired.
 * This is the reason no lock is acquired in each access.
 */
class ScopedReadBuffer extends Slice implements OakScopedReadBuffer, OakUnsafeDirectBuffer {

    ScopedReadBuffer(int headerSize) {
        super(headerSize);
    }

    ScopedReadBuffer(Slice other) {
        super(other);
    }

    protected int getDataOffset(int index) {
        if (index < 0 || index >= getLength()) {
            throw new IndexOutOfBoundsException();
        }
        return getOffset() + index;
    }

    @Override
    public int capacity() {
        return getLength();
    }

    @Override
    public ByteOrder order() {
        return readBuffer.order();
    }

    @Override
    public byte get(int index) {
        return readBuffer.get(getDataOffset(index));
    }

    @Override
    public char getChar(int index) {
        return readBuffer.getChar(getDataOffset(index));
    }

    @Override
    public short getShort(int index) {
        return readBuffer.getShort(getDataOffset(index));
    }

    @Override
    public int getInt(int index) {
        return readBuffer.getInt(getDataOffset(index));
    }

    @Override
    public long getLong(int index) {
        return readBuffer.getLong(getDataOffset(index));
    }

    @Override
    public float getFloat(int index) {
        return readBuffer.getFloat(getDataOffset(index));
    }

    @Override
    public double getDouble(int index) {
        return readBuffer.getDouble(getDataOffset(index));
    }
}
