/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class OakRKeyBufferImpl implements OakRBuffer {

    private final ByteBuffer byteBuffer;
    private final Handle handle;

    OakRKeyBufferImpl(ByteBuffer byteBuffer, Handle handle) {
        this.handle = handle;
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int capacity() {
        handle.start();
        int i = byteBuffer.capacity();
        handle.end();
        return i;
    }

    @Override
    public byte get(int index) {
        handle.start();
        byte b = byteBuffer.get(index);
        handle.end();
        return b;
    }

    @Override
    public ByteOrder order() {
        handle.start();
        ByteOrder bo = byteBuffer.order();
        handle.end();
        return bo;
    }

    @Override
    public char getChar(int index) {
        handle.start();
        char c = byteBuffer.getChar(index);
        handle.end();
        return c;
    }

    @Override
    public short getShort(int index) {
        handle.start();
        short s = byteBuffer.getShort(index);
        handle.end();
        return s;
    }

    @Override
    public int getInt(int index) {
        handle.start();
        int i = byteBuffer.getInt(index);
        handle.end();
        return i;
    }

    @Override
    public long getLong(int index) {
        handle.start();
        long l = byteBuffer.getLong(index);
        handle.end();
        return l;
    }

    @Override
    public float getFloat(int index) {
        handle.start();
        float f = byteBuffer.getFloat(index);
        handle.end();
        return f;
    }

    @Override
    public double getDouble(int index) {
        handle.start();
        double d = byteBuffer.getChar(index);
        handle.end();
        return d;
    }

    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        handle.start();
        T t = transformer.apply(byteBuffer);
        handle.end();
        return t;
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        handle.start();
        UnsafeUtils.unsafeCopyBufferToIntArray(byteBuffer, srcPosition, dstArray, countInts);
        handle.end();
    }

}
