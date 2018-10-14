/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

class Handle<V> implements OakWBuffer {

    final ReentrantReadWriteLock.ReadLock readLock;
    final ReentrantReadWriteLock.WriteLock writeLock;
    ByteBuffer value;
    final AtomicBoolean deleted;

    Handle(ByteBuffer value) {
        this.value = value;
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
        value.rewind();
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
        if (this.value.capacity() < capacity) { // can not reuse the existing space
            memoryManager.release(this.value);
            this.value = memoryManager.allocate(capacity);
        } else {
            this.value.clear(); // zero the position
        }
        serializer.serialize(newVal, this.value);
        value.rewind();
        assert value.position() == 0;
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
            this.value.rewind(); // TODO rewind?
            writeLock.unlock();
        }
        assert value.position() == 0;
        return true;
    }

    @Override
    public OakWBuffer position(int newPosition) {
        assert writeLock.isHeldByCurrentThread();
        value.position(newPosition);
        return this;
    }

    @Override
    public OakWBuffer mark() {
        assert writeLock.isHeldByCurrentThread();
        value.mark();
        return this;
    }

    @Override
    public OakWBuffer reset() {
        assert writeLock.isHeldByCurrentThread();
        value.reset();
        return this;
    }

    @Override
    public OakWBuffer clear() {
        assert writeLock.isHeldByCurrentThread();
        value.clear();
        return this;
    }

    @Override
    public OakWBuffer flip() {
        assert writeLock.isHeldByCurrentThread();
        value.flip();
        return this;
    }

    @Override
    public OakWBuffer rewind() {
        assert writeLock.isHeldByCurrentThread();
        value.rewind();
        return this;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        assert writeLock.isHeldByCurrentThread();
        return value;
    }

    public ByteBuffer getImmutableByteBuffer() {
        //TODO: check that the read lock is held by the current thread
        ByteBuffer readOnlyBB = value.asReadOnlyBuffer();
        // the new read only BB object capacity, limit, position, and mark values
        // will be identical to those of the value buffer (shared with others).
        // for thread-safeness the returned BB need to be rewind
        readOnlyBB.rewind();
        return value.asReadOnlyBuffer();
    }

    @Override
    // increments the position and thus should be used only in write mode
    public byte get() {
        assert writeLock.isHeldByCurrentThread();
        return value.get();
    }

    @Override
    public OakWBuffer put(byte b) {
        assert writeLock.isHeldByCurrentThread();
        value.put(b);
        return this;
    }

    @Override
    public int capacity() {
        return value.capacity();
    }

    int capacityLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int capacity;
        try {
            capacity = value.capacity();
        } finally {
            readLock.unlock();
        }
        return capacity;
    }

    @Override
    public int position() {
        return value.position();
    }

    int positionLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int position;
        try {
            position = value.position();
        } finally {
            readLock.unlock();
        }
        return position;
    }

    @Override
    public int limit() {
        return value.limit();
    }

    int limitLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int limit;
        try {
            limit = value.limit();
        } finally {
            readLock.unlock();
        }
        return limit;
    }

    @Override
    public int remaining() {
        return value.remaining();
    }

    int remainingLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int remaining;
        try {
            remaining = value.remaining();
        } finally {
            readLock.unlock();
        }
        return remaining;
    }

    @Override
    public boolean hasRemaining() {
        return value.hasRemaining();
    }

    boolean hasRemainingLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        boolean hasRemaining;
        try {
            hasRemaining = value.hasRemaining();
        } finally {
            readLock.unlock();
        }
        return hasRemaining;
    }

    @Override
    public byte get(int index) {
        return value.get(index);
    }

    byte getLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        byte b;
        try {
            b = get(index);
        } finally {
            readLock.unlock();
        }
        return b;
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        assert writeLock.isHeldByCurrentThread();
        value.put(index, b);
        return this;
    }

    @Override
    // increments the position and thus should be used only in write mode
    public OakWBuffer get(byte[] dst, int offset, int length) {
        assert writeLock.isHeldByCurrentThread();
        value.get(dst, offset, length);
        return this;
    }

    @Override
    public OakWBuffer put(byte[] src, int offset, int length) {
        assert writeLock.isHeldByCurrentThread();
        value.put(src, offset, length);
        return this;
    }

    @Override
    public OakWBuffer put(byte[] src) {
        assert writeLock.isHeldByCurrentThread();
        value.put(src);
        return this;
    }

    @Override
    public ByteOrder order() {
        return value.order();
    }

    ByteOrder orderLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        ByteOrder order;
        try {
            order = order();
        } finally {
            readLock.unlock();
        }
        return order;
    }

    @Override
    public OakWBuffer order(ByteOrder bo) {
        value.order(bo);
        return this;
    }

    @Override
    public char getChar() {
        return value.getChar();
    }

    @Override
    public OakWBuffer putChar(char value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putChar(value);
        return this;
    }

    @Override
    public char getChar(int index) {
        return value.getChar(index);
    }

    char getCharLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new ConcurrentModificationException();
        }
        char c;
        try {
            c = getChar(index);
        } finally {
            readLock.unlock();
        }
        return c;
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putChar(index, value);
        return this;
    }

    @Override
    public short getShort() {
        return value.getShort();
    }

    @Override
    public OakWBuffer putShort(short value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putShort(value);
        return this;
    }

    @Override
    public short getShort(int index) {
        return value.getShort(index);
    }

    short getShortLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        short s;
        try {
            s = getShort(index);
        } finally {
            readLock.unlock();
        }
        return s;
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putShort(index, value);
        return this;
    }

    @Override
    public int getInt() {
        return value.getInt();
    }

    @Override
    public OakWBuffer putInt(int value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putInt(value);
        return this;
    }

    @Override
    public int getInt(int index) {
        return value.getInt(index);
    }

    int getIntLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int j;
        try {
            j = getInt(index);
        } finally {
            readLock.unlock();
        }
        return j;
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putInt(index, value);
        return this;
    }

    @Override
    public long getLong() {
        return value.getLong();
    }

    @Override
    public OakWBuffer putLong(long value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putLong(value);
        return this;
    }

    @Override
    public long getLong(int index) {
        return value.getLong(index);
    }

    long getLongLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        long l;
        try {
            l = getLong(index);
        } finally {
            readLock.unlock();
        }
        return l;
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putLong(index, value);
        return this;
    }

    @Override
    public float getFloat() {
        return value.getFloat();
    }

    @Override
    public OakWBuffer putFloat(float value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putFloat(value);
        return this;
    }

    @Override
    public float getFloat(int index) {
        return value.getFloat(index);
    }

    float getFloatLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        float f;
        try {
            f = getFloat(index);
        } finally {
            readLock.unlock();
        }
        return f;
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble() {
        return value.getDouble();
    }

    @Override
    public OakWBuffer putDouble(double value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putDouble(value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        return value.getDouble(index);
    }

    double getDoubleLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        double d;
        try {
            d = getDouble(index);
        } finally {
            readLock.unlock();
        }
        return d;
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putDouble(index, value);
        return this;
    }
}
