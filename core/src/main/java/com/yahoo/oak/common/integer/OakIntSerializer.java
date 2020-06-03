/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.integer;

import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;

public class OakIntSerializer implements OakSerializer<Integer> {

    private final int size;

    public OakIntSerializer() {
        this(Integer.BYTES);
    }

    public OakIntSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(Integer value, OakScopedWriteBuffer targetBuffer) {
        targetBuffer.putInt(0, value);
    }

    @Override
    public Integer deserialize(OakScopedReadBuffer serializedValue) {
        return serializedValue.getInt(0);
    }

    @Override
    public int calculateSize(Integer value) {
        return size;
    }
}
