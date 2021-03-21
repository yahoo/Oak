/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.intbuffer;

import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.UnsafeUtils;

import java.nio.ByteBuffer;

public class OakIntBufferSerializer implements OakSerializer<ByteBuffer> {

    private final int size;

    public OakIntBufferSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(ByteBuffer obj, OakScopedWriteBuffer targetBuffer) {
        OakUnsafeDirectBuffer unsafeTarget = (OakUnsafeDirectBuffer) targetBuffer;
        copyBuffer(obj, 0, size, unsafeTarget.getAddress());
    }

    @Override
    public ByteBuffer deserialize(OakScopedReadBuffer byteBuffer) {
        OakUnsafeDirectBuffer unsafeBuffer = (OakUnsafeDirectBuffer) byteBuffer;

        ByteBuffer ret = ByteBuffer.allocate(getSizeBytes());
        copyBuffer(unsafeBuffer.getAddress(), size, ret, 0);
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

    public static void copyBuffer(ByteBuffer src, int srcPos, int srcSize, ByteBuffer dst, int dstPos) {
        int offset = 0;
        for (int i = 0; i < srcSize; i++) {
            int data = src.getInt(srcPos + offset);
            dst.putInt(dstPos + offset, data);
            offset += Integer.BYTES;
        }
    }

    public static void copyBuffer(ByteBuffer src, int srcPos, int srcSize, long dstAddress) {
        for (int i = 0; i < srcSize; i++) {
            int data = src.getInt(srcPos + Integer.BYTES * i);
            UnsafeUtils.getUnsafe().putInt(dstAddress + Integer.BYTES * i, data);            
        }
    }
    
    public static void copyBuffer(long srcAddress, int srcSize, ByteBuffer dst, int dstPos) {
        for (int i = 0; i < srcSize; i++) {
            int data =UnsafeUtils.getUnsafe().getInt(srcAddress + Integer.BYTES * i);       
            dst.putInt(dstPos + Integer.BYTES * i, data);
        }
    }
}
