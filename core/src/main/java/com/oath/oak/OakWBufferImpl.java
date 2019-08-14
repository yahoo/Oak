/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class OakWBufferImpl implements OakWBuffer {

    private final ByteBuffer bb;

    OakWBufferImpl(ByteBuffer bb) {
        this.bb = bb;
    }

    @Override
    public int capacity() {
        return bb.remaining();
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return bb;
    }

    @Override
    public byte get(int index) {
        return bb.get(index);
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        bb.put(index, b);
        return this;
    }

    @Override
    public ByteOrder order() {
        return bb.order();
    }

    @Override
    public char getChar(int index) {
        return bb.getChar(index);
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        bb.putChar(index, value);
        return this;
    }

    @Override
    public short getShort(int index) {
        return bb.getShort(index);
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        bb.putShort(index, value);
        return this;
    }

    @Override
    public int getInt(int index) {
        return bb.getInt(index);
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        bb.putInt(index, value);
        return this;
    }

    @Override
    public long getLong(int index) {
        return bb.getLong(index);
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        bb.putLong(index, value);
        return this;
    }

    @Override
    public float getFloat(int index) {
        return bb.getFloat(index);
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        bb.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        return bb.getDouble(index);
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        bb.putDouble(index, value);
        return this;
    }

}
