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

import static com.oath.oak.ValueUtils.ValueResult.*;

public class OakRValueBuffer implements OakRBuffer, OakUnsafeDirectBuffer {
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

    OakRValueBuffer(long valueReference, int valueVersion, long keyReference, ValueUtils valueOperator,
                        MemoryManager memoryManager, InternalOakMap<?, ?> internalOakMap) {
        this.valueReference = valueReference;
        this.keyReference = keyReference;
        this.version = valueVersion;
        this.valueOperator = valueOperator;
        this.memoryManager = memoryManager;
        this.internalOakMap = internalOakMap;
    }

    private Slice getValueSlice() {
        return EntrySet.buildValueSlice(valueReference, version, memoryManager);
    }

    private int valuePosition() {
        return EntrySet.getValuePosition(valueReference) + valueOperator.getHeaderSize();
    }

    @Override
    public int capacity() {
        return (EntrySet.getValueLength(valueReference) - valueOperator.getHeaderSize());
    }

    @Override
    public byte get(int index) {
        Slice s = start(getValueSlice());
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
        Slice s = start(getValueSlice());
        order = s.getByteBuffer().order();
        end(s);
        return order;
    }

    @Override
    public char getChar(int index) {
        char c;
        Slice s = start(getValueSlice());
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
        Slice s = start(getValueSlice());
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
        Slice s = start(getValueSlice());
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
        Slice s = start(getValueSlice());
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
        Slice s = start(getValueSlice());
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
        Slice s = start(getValueSlice());
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
        EntrySet.LookUp lookUp = internalOakMap.refreshValuePosition(keyReference);
        if (lookUp == null || lookUp.valueSlice == null) {
            throw new ConcurrentModificationException();
        }
        valueReference = lookUp.valueReference;
        version = lookUp.version;
    }

    /*-------------- OakUnsafeRef --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer buff = getValueSlice().getByteBuffer().asReadOnlyBuffer();
        int position = valuePosition();
        int limit = position + capacity();
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
        return capacity();
    }

    @Override
    public long getAddress() {
        ByteBuffer buff = getValueSlice().getByteBuffer();
        long address = ((DirectBuffer) buff).address();
        return address + valuePosition();
    }
}
