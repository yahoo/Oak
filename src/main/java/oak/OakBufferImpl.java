/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteOrder;

public class OakBufferImpl extends OakBuffer {

    private Handle handle;

    OakBufferImpl(Handle handle) {
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

}
