/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.buffer;

import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;

import java.nio.ByteBuffer;

public class OakBufferSerializer implements OakSerializer<ByteBuffer> {

    private final int size;

    public OakBufferSerializer(int size) {
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
        return size * Byte.BYTES;
    }

    public static void copyBuffer(ByteBuffer src, int srcPos, int srcSize, OakScopedWriteBuffer dst, int dstPos) {
        for (int i = 0; i < srcSize; i++) {
            byte data = src.get(srcPos + Byte.BYTES * i);
            dst.put(dstPos + Byte.BYTES * i, data);
        }
    }
    
    public static void copyBuffer(OakScopedReadBuffer src, int srcPos, int srcSize, ByteBuffer dst, int dstPos) {
        for (int i = 0; i < srcSize; i++) {
            byte data = src.get(srcPos + Byte.BYTES * i);
            dst.put(dstPos + Byte.BYTES * i, data);
        }
    }
}
