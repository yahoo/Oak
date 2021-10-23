/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.bytearray;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.common.MurmurHash3;

public class OakByteArraySerializer implements OakSerializer<byte[]> {

    static final int SIZE_OFFSET = Integer.BYTES;

    @Override
    public void serialize(byte[] object, OakScopedWriteBuffer targetBuffer) {
        final int size = object.length;
        putSerializedSize(targetBuffer, size);

        for (int i = 0; i < size; i++) {
            putSerializedByte(targetBuffer, i, object[i]);
        }
    }

    @Override
    public byte[] deserialize(OakScopedReadBuffer oakBuffer) {
        final int size = getSerializedSize(oakBuffer);

        final byte[] object = new byte[size];
        for (int i = 0; i < size; i++) {
            object[i] = getSerializedByte(oakBuffer, i);
        }
        return object;
    }

    @Override
    public int calculateSize(byte[] object) {
        return SIZE_OFFSET + object.length;
    }

    @Override
    public int calculateHash(byte[] object) {
        return MurmurHash3.murmurhash32(object, 0, object.length, 0);
    }

    public static int getSerializedSize(OakBuffer oakBuffer) {
        return oakBuffer.getInt(0);
    }

    public static void putSerializedSize(OakScopedWriteBuffer oakBuffer, int size) {
        oakBuffer.putInt(0, size);
    }

    public static byte getSerializedByte(OakBuffer oakBuffer, int index) {
        return oakBuffer.get(index + SIZE_OFFSET);
    }

    public static void putSerializedByte(OakScopedWriteBuffer oakBuffer, int index, byte b) {
        oakBuffer.put(index + SIZE_OFFSET, b);
    }
}
