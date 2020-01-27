/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// An instance of OakWBufferImpl is only used when the write lock of the value referenced by it is already acquired.
// This is the reason no lock is acquired in each access.
public class OakWBufferImpl implements OakWBuffer {

    private ByteBuffer bb;
    private int dataPos;

    OakWBufferImpl(Slice s, ValueUtils valueOperator) {
        bb = s.getByteBuffer();
        dataPos = bb.position() + valueOperator.getHeaderSize();
    }

    @Override
    public ByteBuffer getByteBuffer() {
        int initPos = bb.position();
        bb.position(dataPos);
        ByteBuffer ret = bb.slice();
        // It is important to return the original buffer to its original state.
        bb.position(initPos);
        return ret;
    }

    @Override
    public int capacity() {
        return bb.limit() - dataPos;
    }

    @Override
    public byte get(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        return bb.get(dataPos + index);
    }

    @Override
    public OakWBuffer put(int index, byte b) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        bb.put(dataPos + index, b);
        return this;
    }

    @Override
    public ByteOrder order() {
        return bb.order();
    }

    @Override
    public char getChar(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        return bb.getChar(dataPos + index);
    }

    @Override
    public OakWBuffer putChar(int index, char value) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        bb.putChar(dataPos + index, value);
        return this;
    }

    @Override
    public short getShort(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        return bb.getShort(dataPos + index);
    }

    @Override
    public OakWBuffer putShort(int index, short value) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        bb.putShort(dataPos + index, value);
        return this;
    }

    @Override
    public int getInt(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        return bb.getInt(dataPos + index);
    }

    @Override
    public OakWBuffer putInt(int index, int value) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        bb.putInt(dataPos + index, value);
        return this;
    }

    @Override
    public long getLong(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        return bb.getLong(dataPos + index);
    }

    @Override
    public OakWBuffer putLong(int index, long value) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        bb.putLong(dataPos + index, value);
        return this;
    }

    @Override
    public float getFloat(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        return bb.getFloat(dataPos + index);
    }

    @Override
    public OakWBuffer putFloat(int index, float value) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        bb.putFloat(dataPos + index, value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        return bb.getDouble(dataPos + index);
    }

    @Override
    public OakWBuffer putDouble(int index, double value) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        bb.putDouble(dataPos + index, value);
        return this;
    }
}
