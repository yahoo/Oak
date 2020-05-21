/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

/**
 * An instance of this buffer is only used when the write lock of the key/value referenced by it is already acquired.
 * This is the reason no lock is acquired in each access.
 */
class OakAttachedWriteBuffer extends OakAttachedReadBuffer implements OakWriteBuffer, OakUnsafeDirectBuffer {

    protected boolean enabled;

    OakAttachedWriteBuffer(Slice s) {
        super(s);
        enabled = true;
    }

    void disable() {
        enabled = false;
    }

    void validateAccess() {
        if (!enabled) {
            throw new RuntimeException("Attached buffer cannot be used outside of its attached scope.");
        }
    }

    @Override
    public OakWriteBuffer put(int index, byte value) {
        validateAccess();
        getDataByteBuffer().put(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putChar(int index, char value) {
        validateAccess();
        getDataByteBuffer().putChar(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putShort(int index, short value) {
        validateAccess();
        getDataByteBuffer().putShort(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putInt(int index, int value) {
        validateAccess();
        getDataByteBuffer().putInt(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putLong(int index, long value) {
        validateAccess();
        getDataByteBuffer().putLong(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putFloat(int index, float value) {
        validateAccess();
        getDataByteBuffer().putFloat(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putDouble(int index, double value) {
        validateAccess();
        getDataByteBuffer().putDouble(getDataOffset(index), value);
        return this;
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        return getDuplicatedWriteByteBuffer();
    }
}
