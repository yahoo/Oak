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

    private int valuePosition(){
        return bb.position() + ValueUtils.VALUE_HEADER_SIZE;
    }

    @Override
    public int capacity() {
        start();
        int capacity = bb.remaining() - ValueUtils.VALUE_HEADER_SIZE;
        end();
        return capacity;
    }

    @Override
    public byte get(int index) {
        start();
        if(index < 0) throw new IndexOutOfBoundsException();
        byte b = bb.get(index + valuePosition());
        end();
        return b;
    }

    @Override
    public ByteOrder order() {
        ByteOrder order;
        start();
        order = bb.order();
        end();
        return order;
    }

    @Override
    public char getChar(int index) {
        char c;
        start();
        if(index < 0) throw new IndexOutOfBoundsException();
        c = bb.getChar(index + valuePosition());
        end();
        return c;
    }

    @Override
    public short getShort(int index) {
        short s;
        start();
        if(index < 0) throw new IndexOutOfBoundsException();
        s = bb.getShort(index + valuePosition());
        end();
        return s;
    }

    @Override
    public int getInt(int index) {
        int i;
        start();
        if(index < 0) throw new IndexOutOfBoundsException();
        i = bb.getInt(index + valuePosition());
        end();
        return i;
    }

    @Override
    public long getLong(int index) {
        long l;
        start();
        if(index < 0) throw new IndexOutOfBoundsException();
        l = bb.getLong(index + valuePosition());
        end();
        return l;
    }

    @Override
    public float getFloat(int index) {
        float f;
        start();
        if(index < 0) throw new IndexOutOfBoundsException();
        f = bb.getFloat(index + valuePosition());
        end();
        return f;
    }

    @Override
    public double getDouble(int index) {
        double d;
        start();
        if(index < 0) throw new IndexOutOfBoundsException();
        d = bb.getDouble(index + valuePosition());
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
        ByteBuffer dup = bb.duplicate();
        dup.position(dup.position() + ValueUtils.VALUE_HEADER_SIZE);
        ValueUtils.unsafeBufferToIntArrayCopy(dup, srcPosition, dstArray, countInts);
        end();
    }

    private void start() {
        if (!ValueUtils.lockRead(bb))
            throw new ConcurrentModificationException();
    }

    private void end() {
        ValueUtils.unlockRead(bb);
    }

}
