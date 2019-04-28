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

class Handle<V> implements OakWBuffer {

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

    boolean remove(MemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        deleted.set(true);
        writeLock.unlock();
        memoryManager.release(value);
        return true;
    }

    boolean put(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        int capacity = serializer.calculateSize(newVal);
        if (this.value.remaining() < capacity) { // can not reuse the existing space
            memoryManager.release(this.value);
            this.value = memoryManager.allocate(capacity);
        }
        serializer.serialize(newVal, this.value.slice());
        writeLock.unlock();

        return true;
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

    /**
     * Applies a transformation under writers locking
     *
     * @param transformer transformation to apply
     * @return Transformation result or null if value is deleted
     */
    <T> T mutatingTransform(Function<ByteBuffer, T> transformer) {
        T result;
        try {
            writeLock.lock();
            if (isDeleted()) {
                // finally clause will handle unlock
                return null;
            }
            result = transformer.apply(getSlicedByteBuffer());
        } finally {
            writeLock.unlock();
        }
        return result;
    }


    void readLock() {
        readLock.lock();
    }

    void readUnLock() {
        readLock.unlock();
    }

    public void unsafeBufferToIntArrayCopy(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getSlicedReadOnlyByteBuffer(), srcPosition, dstArray, countInts);
    }
}
