/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
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
        }
        serializer.serialize(newVal, this.value.slice());
        writeLock.unlock();
    }

    // returns false in case handle was found deleted and compute didn't take place, true otherwise
    boolean compute(Consumer<ByteBuffer> computer) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        try {
            computer.accept(value.slice());
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    public <T> T transform(Function<ByteBuffer, T> transformer) {
        readLock.lock();
        T transformation = transformer.apply(getImmutableByteBuffer());
        readLock.unlock();
        return transformation;
    }

    public ByteBuffer getByteBuffer() {
        assert writeLock.isHeldByCurrentThread();
        return value.slice();
    }

    public ByteBuffer getImmutableByteBuffer() {
        //TODO YONIGO - add warning that buffer can change
        return value.asReadOnlyBuffer().slice();
    }

    public void readLock() {
        readLock.lock();
    }

    public void readUnlock() {
        readLock.unlock();
    }

}



