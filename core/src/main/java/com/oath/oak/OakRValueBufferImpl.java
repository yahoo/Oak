/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ConcurrentModificationException;
import java.util.function.Function;

public class OakRValueBufferImpl implements OakRBuffer {

    private Handle handle;

    OakRValueBufferImpl(Handle handle) {
        this.handle = handle;
    }

    public void setHandle(Handle handle) {
        this.handle = handle;
    }

    @Override
    public int capacity() {
        start();
        int capacity = handle.capacity();
        end();
        return capacity;
    }


    @Override
    public byte get(int index) {
        start();
        byte b = handle.get(index);
        end();
        return b;
    }

    @Override
    public ByteOrder order() {
        ByteOrder order;
        start();
        order = handle.order();
        end();
        return order;
    }

    @Override
    public char getChar(int index) {
        char c;
        start();
        c = handle.getChar(index);
        end();
        return c;
    }

    @Override
    public short getShort(int index) {
        short s;
        start();
        s = handle.getShort(index);
        end();
        return s;
    }

    @Override
    public int getInt(int index) {
        int i;
        start();
        i = handle.getInt(index);
        end();
        return i;
    }

    @Override
    public long getLong(int index) {
        long l;
        start();
        l = handle.getLong(index);
        end();
        return l;
    }

    @Override
    public float getFloat(int index) {
        float f;
        start();
        f = handle.getFloat(index);
        end();
        return f;
    }

    @Override
    public double getDouble(int index) {
        double d;
        start();
        d = handle.getDouble(index);
        end();
        return d;
    }

    /**
     * Returns a transformation of ByteBuffer content.
     *
     * @param transformer the function that executes the transformation
     * @return a transformation of the ByteBuffer content
     * @throws NullPointerException if the transformer is null
     */
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }
        T retVal = (T) handle.transform(transformer);
        if (retVal == null) {
            throw new ConcurrentModificationException();
        }
        return retVal;
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        start();
        handle.unsafeBufferToIntArrayCopy(srcPosition, dstArray, countInts);
        end();
    }

    private void start() {
        handle.readLock();
        if (handle.isDeleted()) {
            handle.readUnLock();
            throw new ConcurrentModificationException();
        }
    }

    private void end() {
        handle.readUnLock();
    }

}
