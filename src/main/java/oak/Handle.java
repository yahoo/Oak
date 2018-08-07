/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

abstract class Handle<K, V> extends WritableOakBuffer {

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

    abstract void increaseValueCapacity(OakMemoryManager memoryManager);

    abstract void setValue(ByteBuffer value, int i);

    boolean isDeleted() {
        return deleted.get();
    }

    abstract boolean remove(OakMemoryManager memoryManager);

    abstract void put(V newVal, ValueSerializer<K, V> serializer, SizeCalculator<V> sizeCalculator, OakMemoryManager memoryManager);

    // returns false in case handle was found deleted and compute didn't take place, true otherwise
    boolean compute(Consumer<ByteBuffer> computer, OakMemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        // TODO: invalidate oak buffer after accept
        try {
            computer.accept(this.value);
        } finally {
            value.rewind(); // TODO rewind?
            writeLock.unlock();
        }
        return true;
    }

    @Override
    public WritableOakBuffer position(int newPosition) {
        assert writeLock.isHeldByCurrentThread();
        value.position(newPosition);
        return this;
    }

    @Override
    public WritableOakBuffer mark() {
        assert writeLock.isHeldByCurrentThread();
        value.mark();
        return this;
    }

    @Override
    public WritableOakBuffer reset() {
        assert writeLock.isHeldByCurrentThread();
        value.reset();
        return this;
    }

    @Override
    public WritableOakBuffer clear() {
        assert writeLock.isHeldByCurrentThread();
        value.clear();
        return this;
    }

    @Override
    public WritableOakBuffer flip() {
        assert writeLock.isHeldByCurrentThread();
        value.flip();
        return this;
    }

    @Override
    public WritableOakBuffer rewind() {
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
        return value.asReadOnlyBuffer();
    }

    @Override
    public byte get() {
        return value.get();
    }

    @Override
    public WritableOakBuffer put(byte b) {
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
    public WritableOakBuffer put(int index, byte b) {
        assert writeLock.isHeldByCurrentThread();
        value.put(index, b);
        return this;
    }

    @Override
    public WritableOakBuffer get(byte[] dst, int offset, int length) {
        value.get(dst, offset, length);
        return this;
    }

    @Override
    public WritableOakBuffer put(byte[] src, int offset, int length) {
        assert writeLock.isHeldByCurrentThread();
        value.put(src, offset, length);
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
    public WritableOakBuffer order(ByteOrder bo) {
        value.order(bo);
        return this;
    }

    @Override
    public char getChar() {
        return value.getChar();
    }

    @Override
    public WritableOakBuffer putChar(char value) {
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
    public WritableOakBuffer putChar(int index, char value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putChar(index, value);
        return this;
    }

    @Override
    public short getShort() {
        return value.getShort();
    }

    @Override
    public WritableOakBuffer putShort(short value) {
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
    public WritableOakBuffer putShort(int index, short value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putShort(index, value);
        return this;
    }

    @Override
    public int getInt() {
        return value.getInt();
    }

    @Override
    public WritableOakBuffer putInt(int value) {
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
    public WritableOakBuffer putInt(int index, int value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putInt(index, value);
        return this;
    }

    @Override
    public long getLong() {
        return value.getLong();
    }

    @Override
    public WritableOakBuffer putLong(long value) {
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
    public WritableOakBuffer putLong(int index, long value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putLong(index, value);
        return this;
    }

    @Override
    public float getFloat() {
        return value.getFloat();
    }

    @Override
    public WritableOakBuffer putFloat(float value) {
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
    public WritableOakBuffer putFloat(int index, float value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble() {
        return value.getDouble();
    }

    @Override
    public WritableOakBuffer putDouble(double value) {
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
    public WritableOakBuffer putDouble(int index, double value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putDouble(index, value);
        return this;
    }

}
