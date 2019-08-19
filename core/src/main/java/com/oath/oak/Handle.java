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

import static com.oath.oak.ValueUtils.*;

class Handle<V> implements OakWBuffer {

    private ByteBuffer bb;
    private long offset;

    Handle() {
        this.bb = null;
        try {
            offset = UnsafeUtils.unsafe.objectFieldOffset(Handle.class.getDeclaredField("bb"));
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
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

    boolean put(V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        if (!ValueUtils.lockWrite(bb)) return false;
        int capacity = serializer.calculateSize(newVal);
        ByteBuffer dup = getActualValueBuffer(bb);
        if (bb.remaining() < capacity) { // can not reuse the existing space
            memoryManager.release(dup);
            dup = memoryManager.allocate(capacity + VALUE_HEADER_SIZE);
            // Init new lock
            dup.putInt(dup.position(), LOCK_LOCKED);
            assert UnsafeUtils.unsafe.compareAndSwapObject(this, offset, bb, dup);
            dup = getActualValueBuffer(bb);
        }
        // Duplicating bb instead
        serializer.serialize(newVal, dup);
        unlockWrite(bb);
        return true;
    }

    // returns false in case handle was found deleted and compute didn't take place, true otherwise
    boolean compute(Consumer<OakWBuffer> computer) {
        return ValueUtils.compute(bb, computer);
    }

    ByteBuffer getSlicedReadOnlyByteBuffer() {
        ByteBuffer dup = bb.asReadOnlyBuffer();
        dup.position(dup.position() + VALUE_HEADER_SIZE);
        return dup.slice();
    }

    /* OakWBuffer interface */

    public int capacity() {
        return bb.remaining() - VALUE_HEADER_SIZE;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return bb;
    }

    public byte get(int index) {
        return bb.get(bb.position() + index + VALUE_HEADER_SIZE);
    }

    public OakWBuffer put(int index, byte b) {
        bb.put(bb.position() + index + VALUE_HEADER_SIZE, b);
        return this;
    }

    public char getChar(int index) {
        return bb.getChar(bb.position() + index + VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        bb.putChar(bb.position() + index + VALUE_HEADER_SIZE, value);
        return this;
    }

    public short getShort(int index) {
        return bb.getShort(bb.position() + index);
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        bb.putShort(bb.position() + index + VALUE_HEADER_SIZE, value);
        return this;
    }

    public int getInt(int index) {
        return bb.getInt(bb.position() + index + VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        bb.putInt(bb.position() + index + VALUE_HEADER_SIZE, value);
        return this;
    }

    public long getLong(int index) {
        return bb.getLong(bb.position() + index + VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        bb.putLong(bb.position() + index + VALUE_HEADER_SIZE, value);
        return this;
    }

    public float getFloat(int index) {
        return bb.getFloat(bb.position() + index + VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        bb.putFloat(bb.position() + index + VALUE_HEADER_SIZE, value);
        return this;
    }

    public double getDouble(int index) {
        return bb.getDouble(bb.position() + index + VALUE_HEADER_SIZE);
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        bb.putDouble(bb.position() + index + VALUE_HEADER_SIZE, value);
        return this;
    }

    public ByteOrder order() {
        return bb.order();
    }

    public <T> T transform(Function<ByteBuffer, T> transformer) {
        return ValueUtils.transform(bb, transformer);
    }

    /**
     * Applies a transformation under writers locking
     *
     * @param transformer transformation to apply
     * @return Transformation result or null if value is deleted
     */
    <T> T mutatingTransform(Function<ByteBuffer, T> transformer) {
        return ValueUtils.mutatingTransform(bb, transformer);
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
