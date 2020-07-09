/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

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
        return buffer.order();
    }

    @Override
    public byte get(int index) {
        return buffer.get(getDataOffset(index));
    }

    @Override
    public char getChar(int index) {
        return buffer.getChar(getDataOffset(index));
    }

    @Override
    public short getShort(int index) {
        return buffer.getShort(getDataOffset(index));
    }

    @Override
    public int getInt(int index) {
        return buffer.getInt(getDataOffset(index));
    }

    @Override
    public long getLong(int index) {
        return buffer.getLong(getDataOffset(index));
    }

    @Override
    public float getFloat(int index) {
        return buffer.getFloat(getDataOffset(index));
    }

    @Override
    public double getDouble(int index) {
        return buffer.getDouble(getDataOffset(index));
    }
}
