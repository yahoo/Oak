/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OakWBufferImpl implements OakWBuffer {

    private Slice s;
    private ByteBuffer bb;
    private final NovaValueOperations operator;

    OakWBufferImpl(Slice s, NovaValueOperations operator) {
        this.s = s;
        this.bb = s.getByteBuffer();
        this.operator = operator;
    }

    private int valuePosition() {
        return bb.position() + operator.getHeaderSize();
    }

    @Override
    public int capacity() {
        return bb.remaining() - operator.getHeaderSize();
    }

    @Override
    public byte get(int index) {
        if (index < 0) throw new IndexOutOfBoundsException();
        return bb.get(index + valuePosition());
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        if (index < 0) throw new IndexOutOfBoundsException();
        bb.put(index + valuePosition(), b);
        return this;
    }

    @Override
    public ByteOrder order() {
        return bb.order();
    }

    @Override
    public char getChar(int index) {
        if (index < 0) throw new IndexOutOfBoundsException();
        return bb.getChar(index + valuePosition());
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        if (index < 0) throw new IndexOutOfBoundsException();
        bb.putChar(index + valuePosition(), value);
        return this;
    }

    @Override
    public short getShort(int index) {
        if (index < 0) throw new IndexOutOfBoundsException();
        return bb.getShort(index + valuePosition());
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        if (index < 0) throw new IndexOutOfBoundsException();
        bb.putShort(index + valuePosition(), value);
        return this;
    }

    @Override
    public int getInt(int index) {
        if (index < 0) throw new IndexOutOfBoundsException();
        return bb.getInt(index + valuePosition());
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        if (index < 0) throw new IndexOutOfBoundsException();
        bb.putInt(index + valuePosition(), value);
        return this;
    }

    @Override
    public long getLong(int index) {
        if (index < 0) throw new IndexOutOfBoundsException();
        return bb.getLong(index + valuePosition());
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        if (index < 0) throw new IndexOutOfBoundsException();
        bb.putLong(index + valuePosition(), value);
        return this;
    }

    @Override
    public float getFloat(int index) {
        if (index < 0) throw new IndexOutOfBoundsException();
        return bb.getFloat(index + valuePosition());
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        if (index < 0) throw new IndexOutOfBoundsException();
        bb.putFloat(index + valuePosition(), value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        if (index < 0) throw new IndexOutOfBoundsException();
        return bb.getDouble(index + valuePosition());
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        if (index < 0) throw new IndexOutOfBoundsException();
        bb.putDouble(index + valuePosition(), value);
        return this;
    }

}
