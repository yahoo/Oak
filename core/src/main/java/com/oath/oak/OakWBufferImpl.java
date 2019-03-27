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

    private final Handle handle;

    OakWBufferImpl(Handle handle) {
        this.handle = handle;
    }


    @Override
    public int capacity() {
        return handle.capacity();
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return handle.getSlicedByteBuffer();
    }

    @Override
    public byte get(int index) {
        return handle.get(index);
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        handle.put(index, b);
        return this;
    }

    @Override
    public ByteOrder order() {
        return handle.order();
    }

    @Override
    public char getChar(int index) {
        return handle.getChar(index);
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        handle.putChar(index, value);
        return this;
    }

    @Override
    public short getShort(int index) {
        return handle.getShort(index);
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        handle.putShort(index, value);
        return this;
    }

    @Override
    public int getInt(int index) {
        return handle.getInt(index);
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        handle.putInt(index, value);
        return this;
    }

    @Override
    public long getLong(int index) {
        return handle.getLong(index);
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        handle.putLong(index, value);
        return this;
    }

    @Override
    public float getFloat(int index) {
        return handle.getFloat(index);
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        handle.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        return handle.getDouble(index);
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        handle.putDouble(index, value);
        return this;
    }

    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        return (T) handle.transform(transformer);
    }

}
