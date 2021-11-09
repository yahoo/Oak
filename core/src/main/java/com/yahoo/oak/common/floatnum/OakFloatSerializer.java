/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.floatnum;


import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.common.MurmurHash3;

public class OakFloatSerializer implements OakSerializer<Float> {

    private final int size;

    public OakFloatSerializer() {
        this(Float.BYTES);
    }

    public OakFloatSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(Float value, OakScopedWriteBuffer targetBuffer) {
        targetBuffer.putFloat(0, value);
    }

    @Override
    public Float deserialize(OakScopedReadBuffer serializedValue) {
        return serializedValue.getFloat(0);
    }

    @Override
    public int calculateSize(Float value) {
        return size;
    }

    @Override
    public int calculateHash(Float object) {
        byte[] byteArray = floatToByteArray(object);
        return MurmurHash3.murmurhash32(byteArray, 0, Float.BYTES, 0);
    }

    private static byte[] floatToByteArray(float value) {
        int intBits =  Float.floatToIntBits(value);
        return new byte[] {
            (byte) (intBits >> 24), (byte) (intBits >> 16), (byte) (intBits >> 8), (byte) (intBits) };
    }
}
