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

    private int valuePosition(){
        return bb.position() + ValueUtils.VALUE_HEADER_SIZE;
    }

    @Override
    public int capacity() {
        return bb.remaining() - ValueUtils.VALUE_HEADER_SIZE;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return bb;
    }

    @Override
    public byte get(int index) {
        return bb.get(index + valuePosition());
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        bb.put(index + valuePosition(), b);
        return this;
    }

    @Override
    public ByteOrder order() {
        return bb.order();
    }

    @Override
    public char getChar(int index) {
        return bb.getChar(index + valuePosition());
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        bb.putChar(index + valuePosition(), value);
        return this;
    }

    @Override
    public short getShort(int index) {
        return bb.getShort(index + valuePosition());
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        bb.putShort(index + valuePosition(), value);
        return this;
    }

    @Override
    public int getInt(int index) {
        return bb.getInt(index + valuePosition());
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        bb.putInt(index + valuePosition(), value);
        return this;
    }

    @Override
    public long getLong(int index) {
        return bb.getLong(index + valuePosition());
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        bb.putLong(index + valuePosition(), value);
        return this;
    }

    @Override
    public float getFloat(int index) {
        return bb.getFloat(index + valuePosition());
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        bb.putFloat(index + valuePosition(), value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        return bb.getDouble(index + valuePosition());
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        bb.putDouble(index + valuePosition(), value);
        return this;
    }

}
