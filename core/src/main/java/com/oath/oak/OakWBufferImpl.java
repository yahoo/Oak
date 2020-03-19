/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

// An instance of OakWBufferImpl is only used when the write lock of the value referenced by it is already acquired.
// This is the reason no lock is acquired in each access.
class OakWBufferImpl extends OakReadBufferWrapper implements OakWBuffer, OakUnsafeDirectBuffer {

    OakWBufferImpl(Slice s, int headerSize) {
        super(s, headerSize);
    }

    OakWBufferImpl(ByteBuffer buff, int headerSize) {
        super(buff, headerSize);
    }

    OakWBufferImpl(ByteBuffer buff, int offset, int limit, int headerSize) {
        super(buff, offset, limit, headerSize);
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        checkIndex(index);
        buff.put(offset + index, b);
        return this;
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        checkIndex(index);
        buff.putChar(offset + index, value);
        return this;
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        checkIndex(index);
        buff.putShort(offset + index, value);
        return this;
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        checkIndex(index);
        buff.putInt(offset + index, value);
        return this;
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        checkIndex(index);
        buff.putLong(offset + index, value);
        return this;
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        checkIndex(index);
        buff.putFloat(offset + index, value);
        return this;
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        checkIndex(index);
        buff.putDouble(offset + index, value);
        return this;
    }

    /*-------------- OakUnsafeRef --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer tempBuff = buff.duplicate();
        tempBuff.position(offset);
        tempBuff.limit(offset + length);
        return tempBuff.slice();
    }
}
