/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class OakRValueBufferImpl implements OakRBuffer {

    private Handle handle;

    OakRValueBufferImpl(Handle handle) {
        this.handle = handle;
    }

    @Override
    public int capacity() {
        return handle.capacityLock();
    }

    @Override
    public int position() {
        return handle.positionLock();
    }

    @Override
    public int limit() {
        return handle.limitLock();
    }

    @Override
    public int remaining() {
        return handle.remainingLock();
    }

    @Override
    public boolean hasRemaining() {
        return handle.hasRemainingLock();
    }

    @Override
    public byte get(int index) {
        return handle.getLock(index);
    }


    /**
     * @throws NullPointerException if the key was removed
     */
    @Override
    public ByteOrder order() {
        return handle.orderLock();
    }

    /**
     * @throws NullPointerException if the key was removed
     */
    @Override
    public char getChar(int index) {
        return handle.getCharLock(index);
    }


    /**
     * @throws NullPointerException if the key was removed
     */
    @Override
    public short getShort(int index) {
        return handle.getShortLock(index);
    }


    /**
     * @throws NullPointerException if the key was removed
     */
    @Override
    public int getInt(int index) {
        return handle.getIntLock(index);
    }


    /**
     * @throws NullPointerException if the key was removed
     */
    @Override
    public long getLong(int index) {
        return handle.getLongLock(index);
    }


    /**
     * @throws NullPointerException if the key was removed
     */
    @Override
    public float getFloat(int index) {
        return handle.getFloatLock(index);
    }


    /**
     * @throws NullPointerException if the key was removed
     */
    @Override
    public double getDouble(int index) {
        return handle.getDoubleLock(index);
    }

    /**
     * @throws NullPointerException if the transformer is null;
     */
    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        T transformation;
        try {
            handle.readLock.lock();
            ByteBuffer value = handle.getImmutableByteBuffer();
            transformation = transformer.apply(value);
        } finally {
            handle.readLock.unlock();
        }
        return transformation;
    }

}
