/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

class Handle<V> implements OakWBuffer {

    final ReentrantReadWriteLock.ReadLock readLock;
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

    void increaseValueCapacity(MemoryManager memoryManager) {
        assert writeLock.isHeldByCurrentThread();
        ByteBuffer newValue = memoryManager.allocate(value.capacity() * 2);
        for (int j = 0; j < value.limit(); j++) {
            newValue.put(j, value.get(j));
        }
        newValue.position(value.position());
        memoryManager.release(this.value);
        value = newValue;
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

    void put(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return;
        }
        int capacity = serializer.calculateSize(newVal);
        if (this.value.remaining() < capacity) { // can not reuse the existing space
            memoryManager.release(this.value);
            this.value = memoryManager.allocate(capacity);
        } else {

        }
        serializer.serialize(newVal, this.value);
        writeLock.unlock();
    }

    // returns false in case handle was found deleted and compute didn't take place, true otherwise
    boolean compute(Consumer<OakWBuffer> computer, MemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        // TODO: invalidate oak buffer after accept
        try {
            OakWBufferImpl oakWBufferImpl = new OakWBufferImpl(this, memoryManager);
            computer.accept(oakWBufferImpl);
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    @Override
    public OakWBuffer position(int newPosition) {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer mark() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer reset() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer clear() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer flip() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer rewind() {
        throw new NotImplementedException();
    }

    @Override
    public ByteBuffer getByteBuffer() {
        throw new NotImplementedException();
    }

    public ByteBuffer getImmutableByteBuffer() {
        //TODO: check that the read lock is held by the current thread
        return value.asReadOnlyBuffer();
    }

    @Override
    // increments the position and thus should be used only in write mode
    public byte get() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer put(byte b) {
        throw new NotImplementedException();
    }

    @Override
    public int capacity() {
        throw new NotImplementedException();
    }

    @Override
    public int position() {
        throw new NotImplementedException();
    }

    @Override
    public int limit() {
        throw new NotImplementedException();
    }


    @Override
    public int remaining() {
        throw new NotImplementedException();
    }


    @Override
    public boolean hasRemaining() {
        return value.hasRemaining();
    }

    @Override
    public byte get(int index) {
        return value.get(value.position() + index);
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        assert writeLock.isHeldByCurrentThread();
        value.put(value.position() + index, b);
        return this;
    }

    @Override
    // increments the position and thus should be used only in write mode
    public OakWBuffer get(byte[] dst, int offset, int length) {
        assert writeLock.isHeldByCurrentThread();
        value.get(dst, value.position() + offset, length);
        return this;
    }

    @Override
    public OakWBuffer put(byte[] src, int offset, int length) {
        assert writeLock.isHeldByCurrentThread();
        value.put(src, value.position() + offset, length);
        return this;
    }

    @Override
    public OakWBuffer put(byte[] src) {
        throw new NotImplementedException();
    }

    @Override
    public ByteOrder order() {
        return value.order();
    }

    @Override
    public OakWBuffer order(ByteOrder bo) {
        value.order(bo);
        return this;
    }

    @Override
    public char getChar() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer putChar(char value) {
        throw new NotImplementedException();
    }

    @Override
    public char getChar(int index) {
        return value.getChar(value.position() + index);
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putChar(this.value.position() + index, value);
        return this;
    }

    @Override
    public short getShort() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer putShort(short value) {
        throw new NotImplementedException();
    }

    @Override
    public short getShort(int index) {
        return value.getShort(value.position() + index);
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putShort(this.value.position() + index, value);
        return this;
    }

    @Override
    public int getInt() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer putInt(int value) {
        throw new NotImplementedException();
    }

    @Override
    public int getInt(int index) {
        return value.getInt(value.position() + index);
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putInt(this.value.position() + index, value);
        return this;
    }

    @Override
    public long getLong() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer putLong(long value) {
        throw new NotImplementedException();
    }

    @Override
    public long getLong(int index) {
        return value.getLong(value.position() + index);
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putLong(this.value.position() + index, value);
        return this;
    }

    @Override
    public float getFloat() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer putFloat(float value) {
        throw new NotImplementedException();
    }

    @Override
    public float getFloat(int index) {
        return value.getFloat(value.position() + index);
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putFloat(this.value.position() + index, value);
        return this;
    }

    @Override
    public double getDouble() {
        throw new NotImplementedException();
    }

    @Override
    public OakWBuffer putDouble(double value) {
        throw new NotImplementedException();
    }

    @Override
    public double getDouble(int index) {
        return value.getDouble(value.position() + index);
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putDouble(this.value.position() + index, value);
        return this;
    }
}
