/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

import java.nio.ByteOrder;
import java.util.function.Consumer;
import java.util.function.Function;

class Handle implements OakWBuffer {

    private ByteBuffer bb;

    Handle() {
        this.bb = null;
    }

    void setValue(ByteBuffer value) {
        //writeLock.lock();
        this.bb = value;
        //writeLock.unlock();
    }

    boolean isDeleted() {
        return ValueUtils.isValueDeleted(bb);
    }

    boolean remove(MemoryManager memoryManager) {
        return ValueUtils.remove(bb, memoryManager);
    }

    <V> boolean put(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        return ValueUtils.put(bb, newVal, serializer, memoryManager);
    }

    // returns false in case handle was found deleted and compute didn't take place, true otherwise
    boolean compute(Consumer<OakWBuffer> computer) {
        return ValueUtils.compute(bb, computer);
    }

    ByteBuffer getSlicedReadOnlyByteBuffer() {
        ByteBuffer dup = bb.asReadOnlyBuffer();
        dup.position(dup.position() + ValueUtils.VALUE_HEADER_SIZE);
        return dup.slice();
    }

    /* OakWBuffer interface */

    public int capacity() {
        return bb.remaining() - ValueUtils.VALUE_HEADER_SIZE;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return bb;
    }

    public byte get(int index) {
        return bb.get(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE);
    }

    public OakWBuffer put(int index, byte b) {
        bb.put(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE, b);
        return this;
    }

    public char getChar(int index) {
        return bb.getChar(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        bb.putChar(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE, value);
        return this;
    }

    public short getShort(int index) {
        return bb.getShort(bb.position() + index);
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        bb.putShort(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE, value);
        return this;
    }

    public int getInt(int index) {
        return bb.getInt(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        bb.putInt(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE, value);
        return this;
    }

    public long getLong(int index) {
        return bb.getLong(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        bb.putLong(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE, value);
        return this;
    }

    public float getFloat(int index) {
        return bb.getFloat(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        bb.putFloat(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE, value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        return bb.getDouble(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        bb.putDouble(bb.position() + index + ValueUtils.VALUE_HEADER_SIZE, value);
        return this;
    }

    public ByteOrder order() {
        return bb.order();
    }

    public <T> T transform(Function<ByteBuffer, T> transformer) {
        return ValueUtils.transform(bb, transformer);
    }

    <V> V exchange(V newValue, Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer, MemoryManager memoryManager) {
        try {
            writeLock.lock();
            if (isDeleted())
                return null;
            V v = valueDeserializeTransformer.apply(this.value);
            innerPut(newValue, serializer, memoryManager);
            return v;
        } finally {
            writeLock.unlock();
        }
    }

    <V> boolean compareExchange(V oldValue, V newValue, Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer, MemoryManager memoryManager) {
        try {
            writeLock.lock();
            if (isDeleted())
                return false;
            V v = valueDeserializeTransformer.apply(this.value);
            if (!v.equals(oldValue))
                return false;
            innerPut(newValue, serializer, memoryManager);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    boolean readLock() {
        return ValueUtils.lockRead(bb);
    }

    void readUnlock() {
        ValueUtils.unlockRead(bb);
    }

    public void unsafeBufferToIntArrayCopy(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getSlicedReadOnlyByteBuffer(), srcPosition, dstArray, countInts);
    }
}
