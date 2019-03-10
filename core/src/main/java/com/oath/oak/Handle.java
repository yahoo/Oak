/**
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

class Handle<V> {

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
    boolean compute(Consumer<ByteBuffer> computer) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        try {
            computer.accept(getSlicedByteBuffer());
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    public ByteBuffer getSlicedByteBuffer() {
        assert writeLock.isHeldByCurrentThread();
        return value.slice();
    }

    public ByteBuffer getSlicedReadOnlyByteBuffer() {
        //TODO: check that the read lock is held by the current thread
        return value.asReadOnlyBuffer().slice();
    }

    public int capacity() {
        return value.remaining();
    }

    public byte get(int index) {
        return value.get(value.position() + index);
    }
    public char getChar(int index) {
        return value.getChar(value.position() + index);
    }
    public short getShort(int index) {
        return value.getShort(value.position() + index);
    }
    public int getInt(int index) {
        return value.getInt(value.position() + index);
    }
    public long getLong(int index) {
        return value.getLong(value.position() + index);
    }
    public float getFloat(int index) {
        return value.getFloat(value.position() + index);
    }
    public double getDouble(int index) {
        return value.getDouble(value.position() + index);
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
     * @param transformer
     * @return Transformation result or null if value is deleted
     */
    <T> T mutatingTransform(Function<ByteBuffer, T> transformer) {
        T result;
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return null;
        }
        try {
            result = transformer.apply(getSlicedByteBuffer());
        } finally {
            writeLock.unlock();
        }
        return result;
    }



    public void readLock() {
        readLock.lock();
    }

    public void readUnLock() {
        readLock.unlock();
    }
}
