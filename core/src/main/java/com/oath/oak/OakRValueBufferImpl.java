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

// remove header
public class OakRValueBufferImpl implements OakRBuffer {

    private final ByteBuffer bb;

    OakRValueBufferImpl(ByteBuffer bb) {
        this.bb = bb;
    }

    @Override
    public int capacity() {
        start();
        int capacity = bb.remaining();
        end();
        return capacity;
    }


    @Override
    public byte get(int index) {
        start();
        byte b = ValueUtils.getActualValueBuffer(bb).get(index);
        end();
        return b;
    }

    @Override
    public ByteOrder order() {
        ByteOrder order;
        start();
        order = ValueUtils.getActualValueBuffer(bb).order();
        end();
        return order;
    }

    @Override
    public char getChar(int index) {
        char c;
        start();
        c = ValueUtils.getActualValueBuffer(bb).getChar(index);
        end();
        return c;
    }

    @Override
    public short getShort(int index) {
        short s;
        start();
        s = ValueUtils.getActualValueBuffer(bb).getShort(index);
        end();
        return s;
    }

    @Override
    public int getInt(int index) {
        int i;
        start();
        i = ValueUtils.getActualValueBuffer(bb).getInt(index);
        end();
        return i;
    }

    @Override
    public long getLong(int index) {
        long l;
        start();
        l = ValueUtils.getActualValueBuffer(bb).getLong(index);
        end();
        return l;
    }

    @Override
    public float getFloat(int index) {
        float f;
        start();
        f = ValueUtils.getActualValueBuffer(bb).getFloat(index);
        end();
        return f;
    }

    @Override
    public double getDouble(int index) {
        double d;
        start();
        d = ValueUtils.getActualValueBuffer(bb).getDouble(index);
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
        T retVal = ValueUtils.transform(bb, transformer);
        if (retVal == null) {
            throw new ConcurrentModificationException();
        }
        return retVal;
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        start();
        ValueUtils.unsafeBufferToIntArrayCopy(ValueUtils.getActualValueBuffer(bb), srcPosition, dstArray, countInts);
        end();
    }

    private void start() {
        if(!ValueUtils.lockRead(bb))
            throw new ConcurrentModificationException();
    }

    private void end() {
        ValueUtils.unlockRead(bb);
    }

}
