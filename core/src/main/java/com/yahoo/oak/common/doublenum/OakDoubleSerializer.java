/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.doublenum;


import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;

public class OakDoubleSerializer implements OakSerializer<Double> {

    private final int size;

    public OakDoubleSerializer() {
        this(Double.BYTES);
    }

    public OakDoubleSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(Double value, OakScopedWriteBuffer targetBuffer) {
        targetBuffer.putDouble(0, value);
    }

    @Override
    public Double deserialize(OakScopedReadBuffer serializedValue) {
        return serializedValue.getDouble(0);
    }

    @Override
    public int calculateSize(Double value) {
        return size;
    }
}
