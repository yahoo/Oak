/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

// An instance of OakWBufferScoped is only used when the write lock of the value referenced by it is already acquired.
// This is the reason no lock is acquired in each access.
class OakAttachedWriteBuffer extends OakAttachedReadBuffer implements OakWriteBuffer, OakUnsafeDirectBuffer {

    OakAttachedWriteBuffer(Slice s, int headerSize) {
        super(s, headerSize);
    }

    @Override
    public OakWriteBuffer put(int index, byte b) {
        checkIndex(index);
        bb.put(dataPos + index, b);
        return this;
    }

    @Override
    public OakWriteBuffer putChar(int index, char value) {
        checkIndex(index);
        bb.putChar(dataPos + index, value);
        return this;
    }

    @Override
    public OakWriteBuffer putShort(int index, short value) {
        checkIndex(index);
        bb.putShort(dataPos + index, value);
        return this;
    }

    @Override
    public OakWriteBuffer putInt(int index, int value) {
        checkIndex(index);
        bb.putInt(dataPos + index, value);
        return this;
    }

    @Override
    public OakWriteBuffer putLong(int index, long value) {
        checkIndex(index);
        bb.putLong(dataPos + index, value);
        return this;
    }

    @Override
    public OakWriteBuffer putFloat(int index, float value) {
        checkIndex(index);
        bb.putFloat(dataPos + index, value);
        return this;
    }

    @Override
    public OakWriteBuffer putDouble(int index, double value) {
        checkIndex(index);
        bb.putDouble(dataPos + index, value);
        return this;
    }

    /*-------------- OakUnsafeRef --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        int initPos = bb.position();
        bb.position(dataPos);
        ByteBuffer ret = bb.slice();
        // It is important to return the original buffer to its original state.
        bb.position(initPos);
        return ret;
    }
}
