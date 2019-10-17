/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.function.Function;

import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.NovaValueUtils.Result.*;

// remove header
public class OakRValueBufferImpl implements OakRBuffer {
    private long valueReference;
    private final long keyReference;
    private int version;
    private final NovaValueOperations operator;
    private final NovaManager memoryManager;
    private final InternalOakMap<?, ?> internalOakMap;

    OakRValueBufferImpl(long valueReference, int valueVersion, long keyReference, NovaValueOperations operator,
                        NovaManager memoryManager, InternalOakMap<?, ?> internalOakMap) {
        this.valueReference = valueReference;
        this.keyReference = keyReference;
        this.version = valueVersion;
        this.operator = operator;
        this.memoryManager = memoryManager;
        this.internalOakMap = internalOakMap;
    }

    private Slice getValueSlice() {
        int[] valueArray = UnsafeUtils.longToInts(valueReference);
        return memoryManager.getSliceFromBlockID(valueArray[0] >>> VALUE_BLOCK_SHIFT, valueArray[1],
                valueArray[0] & VALUE_LENGTH_MASK);
    }

    private ByteBuffer getByteBuffer() {
        return getValueSlice().getByteBuffer();
    }

    private int valuePosition() {
        return UnsafeUtils.longToInts(valueReference)[1] + operator.getHeaderSize();
    }

    @Override
    public int capacity() {
        return (UnsafeUtils.longToInts(valueReference)[0] & VALUE_LENGTH_MASK) - operator.getHeaderSize();
    }


    @Override
    public byte get(int index) {
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        byte b = s.getByteBuffer().get(index + valuePosition());
        end(s);
        return b;
    }

    @Override
    public ByteOrder order() {
        ByteOrder order;
        Slice s = getValueSlice();
        start(s);
        order = s.getByteBuffer().order();
        end(s);
        return order;
    }

    @Override
    public char getChar(int index) {
        char c;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        c = s.getByteBuffer().getChar(index + valuePosition());
        end(s);
        return c;
    }

    @Override
    public short getShort(int index) {
        short i;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        i = s.getByteBuffer().getShort(index + valuePosition());
        end(s);
        return i;
    }

    @Override
    public int getInt(int index) {
        int i;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        i = s.getByteBuffer().getInt(index + valuePosition());
        end(s);
        return i;
    }

    @Override
    public long getLong(int index) {
        long l;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        l = s.getByteBuffer().getLong(index + valuePosition());
        end(s);
        return l;
    }

    @Override
    public float getFloat(int index) {
        float f;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        f = s.getByteBuffer().getFloat(index + valuePosition());
        end(s);
        return f;
    }

    @Override
    public double getDouble(int index) {
        double d;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        d = s.getByteBuffer().getDouble(index + valuePosition());
        end(s);
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
        Map.Entry<NovaValueUtils.Result, T> result = operator.transform(getValueSlice(), transformer, version);
        if (result.getKey() == FALSE) {
            throw new ConcurrentModificationException();
        } else if (result.getKey() == RETRY) {
            lookupValueReference();
            transform(transformer);
        }
        return result.getValue();
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        Slice s = getValueSlice();
        start(s);
        ByteBuffer dup = operator.getValueByteBufferNoHeader(s);
        operator.unsafeBufferToIntArrayCopy(dup, srcPosition, dstArray, countInts);
        end(s);
    }

    private void start(Slice valueSlice) {
        NovaValueUtils.Result res = operator.lockRead(valueSlice, version);
        if (res == FALSE) {
            throw new ConcurrentModificationException();
        }
        if (res == RETRY) {
            lookupValueReference();
            start(getValueSlice());
        }
    }

    private void end(Slice valueSlice) {
        operator.unlockRead(valueSlice, version);
    }

    private void lookupValueReference(){
        Chunk.LookUp lookUp = internalOakMap.getValueFromIndex(keyReference);
        if (lookUp == null || lookUp.valueSlice == null) {
            throw new ConcurrentModificationException();
        }
        valueReference = lookUp.valueReference;
        version = lookUp.version;
    }
}
