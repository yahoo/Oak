/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

class Handle implements OakWBuffer {

    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private ByteBuffer value;
    private final AtomicBoolean deleted;

    Handle() {
        this.value = null;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
        this.deleted = new AtomicBoolean(false);
    }

    void setValue(ByteBuffer value) {
        writeLock.lock();
        this.value = value;
        writeLock.unlock();
    }

    boolean isDeleted() {
        return deleted.get();
    }

    <V> Result<V> remove(MemoryManager memoryManager, V oldValue, Function<ByteBuffer, V> transformer) {
        Result<V> res = Result.withFlag(false);
        writeLock.lock();
        try {
            if (!isDeleted()) {
                V vv = (transformer != null) ? transformer.apply(value) : null;
                if ((oldValue != null) && (!oldValue.equals(vv))) {
                    res = Result.withFlag(false);
                } else {
                    deleted.set(true);
                    res = (transformer != null) ? Result.withValue(vv) : Result.withFlag(true);
                    memoryManager.release(value);
                }
            }
        } finally {
            writeLock.unlock();
        }
        return res;
    }

    <V> boolean put(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        innerPut(newVal, serializer, memoryManager);
        writeLock.unlock();

        return true;
    }

    private <V> void innerPut(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        int capacity = serializer.calculateSize(newVal);
        if (this.value.remaining() < capacity) { // can not reuse the existing space
            memoryManager.release(this.value);
            this.value = memoryManager.allocate(capacity);
        }
        serializer.serialize(newVal, this.value.slice());
    }

    // returns false in case handle was found deleted and compute didn't take place, true otherwise
    boolean compute(Consumer<OakWBuffer> computer) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        try {
            OakWBuffer wBuffer = new OakWBufferImpl(this);
            computer.accept(wBuffer);
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    ByteBuffer getSlicedByteBuffer() {
        assert writeLock.isHeldByCurrentThread();
        return value.slice();
    }

    ByteBuffer getSlicedReadOnlyByteBuffer() {
        //TODO: check that the read lock is held by the current thread
        return value.asReadOnlyBuffer().slice();
    }

    /* OakWBuffer interface */

    public int capacity() {
        return value.remaining();
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return getSlicedByteBuffer();
    }

    public byte get(int index) {
        return value.get(value.position() + index);
    }

    public OakWBuffer put(int index, byte b) {
        assert writeLock.isHeldByCurrentThread();
        value.put(this.value.position() + index, b);
        return this;
    }

    public char getChar(int index) {
        return value.getChar(value.position() + index);
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putChar(this.value.position() + index, value);
        return this;
    }

    public short getShort(int index) {
        return value.getShort(value.position() + index);
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putShort(this.value.position() + index, value);
        return this;
    }

    public int getInt(int index) {
        return value.getInt(value.position() + index);
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putInt(this.value.position() + index, value);
        return this;
    }

    public long getLong(int index) {
        return value.getLong(value.position() + index);
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putLong(this.value.position() + index, value);
        return this;
    }

    public float getFloat(int index) {
        return value.getFloat(value.position() + index);
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putFloat(this.value.position() + index, value);
        return this;
    }

    public double getDouble(int index) {
        return value.getDouble(value.position() + index);
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putDouble(this.value.position() + index, value);
        return this;
    }

    public ByteOrder order() {
        return value.order();
    }

    public <T> T transform(Function<ByteBuffer, T> transformer) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            return null;
        }
        T transformation = transformer.apply(getSlicedReadOnlyByteBuffer());
        readLock.unlock();
        return transformation;
    }

    <V> Result<V> exchange(V newValue, Function<ByteBuffer, V> transformer, OakSerializer<V> serializer,
                           MemoryManager memoryManager) {
        try {
            writeLock.lock();
            if (isDeleted()) {
                return Result.withFlag(false);
            }
            Result<V> res = transformer != null ?
                    Result.withValue(transformer.apply(this.value)) :
                    Result.withFlag(true);
            innerPut(newValue, serializer, memoryManager);
            return res;
        } finally {
            writeLock.unlock();
        }
    }

    <V> boolean compareExchange(V oldValue, V newValue, Function<ByteBuffer, V> valueDeserializeTransformer,
                                OakSerializer<V> serializer, MemoryManager memoryManager) {
        try {
            writeLock.lock();
            if (isDeleted()) {
                return false;
            }
            V v = valueDeserializeTransformer.apply(this.value);
            if (!v.equals(oldValue)) {
                return false;
            }
            innerPut(newValue, serializer, memoryManager);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    void readLock() {
        readLock.lock();
    }

    void readUnLock() {
        readLock.unlock();
    }

    void unsafeBufferToIntArrayCopy(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getSlicedReadOnlyByteBuffer(), srcPosition, dstArray, countInts);
    }
}
