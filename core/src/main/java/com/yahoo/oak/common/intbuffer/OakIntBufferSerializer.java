/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.intbuffer;

import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;

import java.nio.ByteBuffer;

public class OakIntBufferSerializer implements OakSerializer<ByteBuffer> {

    private final int size;

    public OakIntBufferSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(ByteBuffer obj, OakScopedWriteBuffer targetBuffer) {
        copyBuffer(obj, 0, size, targetBuffer, 0);
    }

    @Override
    public ByteBuffer deserialize(OakScopedReadBuffer byteBuffer) {
        ByteBuffer ret = ByteBuffer.allocate(getSizeBytes());
        copyBuffer(byteBuffer, 0, size, ret, 0);
        ret.position(0);
        return ret;
    }

    @Override
    public int calculateSize(ByteBuffer buff) {
        return getSizeBytes();
    }

    public int getSizeBytes() {
        return size * Integer.BYTES;
    }

    public static void copyBuffer(ByteBuffer src, int srcPos, int srcSize, OakScopedWriteBuffer dst, int dstPos) {
        int offset = 0;
        for (int i = 0; i < srcSize; i++) {
            int data = src.getInt(srcPos + offset);
            dst.putInt(dstPos + offset, data);
            offset += Integer.BYTES;
        }
    }
    
    public static void copyBuffer(OakScopedReadBuffer src, int srcPos, int srcSize, ByteBuffer dst, int dstPos) {
        int offset = 0;
        for (int i = 0; i < srcSize; i++) {
            int data = src.getInt(srcPos + offset);       
            dst.putInt(dstPos + offset, data);
            offset += Integer.BYTES;
        }
    }
}
