/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

class Handle<V> {

    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private ByteBuffer value;

    Handle() {
        this.value = null;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    void setValue(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        int capacity = serializer.calculateSize(newVal);
        this.value = memoryManager.allocate(capacity);
        serializer.serialize(newVal, this.value.slice());
    }

    boolean isDeleted() {
        readLock.lock();
        boolean retval = value == null;
        readLock.unlock();
        return retval;
    }

    boolean remove(MemoryManager memoryManager) {
        writeLock.lock();
        if (value == null) {
            writeLock.unlock();
            return false;
        }
        memoryManager.release(value);
        value = null;
        writeLock.unlock();
        return true;
    }

    boolean put(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        writeLock.lock();
        boolean newValue = value == null;
        int capacity = serializer.calculateSize(newVal);
        if (newValue || this.value.remaining() < capacity) { // can not reuse the existing space
            if (!newValue) {
                memoryManager.release(this.value);
            }
            this.value = memoryManager.allocate(capacity);
        }
        serializer.serialize(newVal, this.value.slice());
        writeLock.unlock();
        return newValue;
    }


    boolean putIfAbsent(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        writeLock.lock();
        if (value == null) {
            int capacity = serializer.calculateSize(newVal);
            this.value = memoryManager.allocate(capacity);
            serializer.serialize(newVal, this.value.slice());
            writeLock.unlock();
            return true;
        }
        writeLock.unlock();
        return false;
    }


    boolean putIfAbsentComputeIfPresent(V newVal,
                                        OakSerializer<V> serializer,
                                        Consumer<ByteBuffer> computer,
                                        MemoryManager memoryManager) {
        writeLock.lock();
        if (value == null) {
            int capacity = serializer.calculateSize(newVal);
            this.value = memoryManager.allocate(capacity);
            serializer.serialize(newVal, this.value.slice());
            writeLock.unlock();
            return true;
        } else {
            computer.accept(getSlicedByteBuffer());
            writeLock.unlock();
            return false;
        }
    }


    // returns false in case handle was found deleted and compute didn't take place, true otherwise
    boolean compute(Consumer<ByteBuffer> computer) {
        writeLock.lock();
        if (value == null) {
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
        if (value == null) {
            readLock.unlock();
            return null;
        }
        T transformation = transformer.apply(getSlicedReadOnlyByteBuffer());
        readLock.unlock();
        return transformation;
    }


    public void readLock() {
        readLock.lock();
    }

    public void readUnLock() {
        readLock.unlock();
    }
}
