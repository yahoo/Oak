/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ConcurrentModificationException;
import java.util.function.Function;

import static com.oath.oak.Chunk.BLOCK_ID_LENGTH_ARRAY_INDEX;
import static com.oath.oak.Chunk.POSITION_ARRAY_INDEX;
import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.ValueUtils.ValueResult.*;

class OakRValueBufferImpl implements OakRBuffer, OakUnsafeDirectBuffer {
    /**
     * These are the fields used when accessing the value stored in this buffer (the reference to it in the off-heap,
     * and the version we expect the value to have.
     */
    private long valueReference;
    private int version;
    /**
     * In case the version of the value pointed by {@code valueReference} does not match {@code version}, we assume
     * the value was moved and thus issue a search for this value. For that reason we have this field of the original
     * key of the original value. If the value was moved, using this key we are able to find it in Oak, or determine
     * it was deleted.
     */
    private final long keyReference;
    private final ValueUtils valueOperator;
    /**
     * Since not the actual ByteBuffer is stored, but rather the reference to it, we use the memory manager to
     * reconstruct the ByteBuffer on demand.
     */
    private final MemoryManager memoryManager;
    /**
     * In case of a search, this is the map we search in.
     */
    private final InternalOakMap<?, ?> internalOakMap;

    OakRValueBufferImpl(long valueReference, int valueVersion, long keyReference, ValueUtils valueOperator,
                        MemoryManager memoryManager, InternalOakMap<?, ?> internalOakMap) {
        this.valueReference = valueReference;
        this.keyReference = keyReference;
        this.version = valueVersion;
        this.valueOperator = valueOperator;
        this.memoryManager = memoryManager;
        this.internalOakMap = internalOakMap;
    }

    private Slice getValueSlice() {
        int[] valueArray = UnsafeUtils.longToInts(valueReference);
        return memoryManager.getSliceFromBlockID(valueArray[BLOCK_ID_LENGTH_ARRAY_INDEX] >>> VALUE_BLOCK_SHIFT,
                valueArray[POSITION_ARRAY_INDEX], valueArray[BLOCK_ID_LENGTH_ARRAY_INDEX] & VALUE_LENGTH_MASK);
    }

    private int valuePosition(Slice s) {
        return s.getPosition() + valueOperator.getHeaderSize();
    }

    private int valueLength(Slice s) {
        return s.getLimit() - valuePosition(s);
    }

    @Override
    public int capacity() {
        Slice s = start(getValueSlice());
        int ret = valueLength(s);
        end(s);
        return ret;
    }

    protected void checkIndex(Slice s, int index) {
        if (index < 0 || index >= valueLength(s)) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public byte get(int index) {
        Slice s = start(getValueSlice());
        checkIndex(s, index);
        byte b = s.getByteBuffer().get(index + valuePosition(s));
        end(s);
        return b;
    }

    @Override
    public ByteOrder order() {
        Slice s = start(getValueSlice());
        ByteOrder order = s.getByteBuffer().order();
        end(s);
        return order;
    }

    @Override
    public char getChar(int index) {
        Slice s = start(getValueSlice());
        checkIndex(s, index);
        char c = s.getByteBuffer().getChar(index + valuePosition(s));
        end(s);
        return c;
    }

    @Override
    public short getShort(int index) {
        Slice s = start(getValueSlice());
        checkIndex(s, index);
        short i = s.getByteBuffer().getShort(index + valuePosition(s));
        end(s);
        return i;
    }

    @Override
    public int getInt(int index) {
        Slice s = start(getValueSlice());
        checkIndex(s, index);
        int i = s.getByteBuffer().getInt(index + valuePosition(s));
        end(s);
        return i;
    }

    @Override
    public long getLong(int index) {
        Slice s = start(getValueSlice());
        checkIndex(s, index);
        long l = s.getByteBuffer().getLong(index + valuePosition(s));
        end(s);
        return l;
    }

    @Override
    public float getFloat(int index) {
        Slice s = start(getValueSlice());
        checkIndex(s, index);
        float f = s.getByteBuffer().getFloat(index + valuePosition(s));
        end(s);
        return f;
    }

    @Override
    public double getDouble(int index) {
        Slice s = start(getValueSlice());
        checkIndex(s, index);
        double d = s.getByteBuffer().getDouble(index + valuePosition(s));
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
    public <T> T transform(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }
        // Use a "for" loop to ensure maximal retries.
        for (int i = 0; i < 1024; i++) {
            Result<T> result = valueOperator.transform(getValueSlice(), transformer, version);
            if (result.operationResult == FALSE) {
                throw new ConcurrentModificationException();
            } else if (result.operationResult == RETRY) {
                lookupValueReference();
                continue;
            }
            return result.value;
        }

        throw new RuntimeException("Transform failed: reached retry limit (1024).");
    }

    private Slice start(Slice valueSlice) {
        ValueUtils.ValueResult res = valueOperator.lockRead(valueSlice, version);
        if (res == FALSE) {
            throw new ConcurrentModificationException();
        }
        // In case the value moved or was the version does not match
        if (res == RETRY) {
            lookupValueReference();
            return start(getValueSlice());
        }

        return valueSlice;
    }

    private void end(Slice valueSlice) {
        valueOperator.unlockRead(valueSlice, version);
    }

    private void lookupValueReference() {
        Chunk.LookUp lookUp = internalOakMap.getValueFromIndex(keyReference);
        if (lookUp == null || lookUp.valueSlice == null) {
            throw new ConcurrentModificationException();
        }
        valueReference = lookUp.valueReference;
        version = lookUp.version;
    }

    /*-------------- OakUnsafeRef --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        Slice s = getValueSlice();
        ByteBuffer buff = s.getByteBuffer().asReadOnlyBuffer();
        int position = valuePosition(s);
        int limit = s.getLimit();
        buff.position(position);
        buff.limit(limit);
        return buff.slice();
    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return valueLength(getValueSlice());
    }

    @Override
    public long getAddress() {
        Slice s = getValueSlice();
        ByteBuffer buff = s.getByteBuffer();
        long address = ((DirectBuffer) buff).address();
        return address + valuePosition(s);
    }
}
