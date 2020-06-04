/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.string;

import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;

public class OakStringSerializer implements OakSerializer<String> {

    @Override
    public void serialize(String object, OakScopedWriteBuffer targetBuffer) {
        final int size = object.length();

        targetBuffer.putInt(0, size);
        for (int i = 0; i < object.length(); i++) {
            char c = object.charAt(i);
            targetBuffer.putChar(Integer.BYTES + i * Character.BYTES, c);
        }
    }

    @Override
    public String deserialize(OakScopedReadBuffer byteBuffer) {
        final int size = byteBuffer.getInt(0);

        StringBuilder object = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = byteBuffer.getChar(Integer.BYTES + i * Character.BYTES);
            object.append(c);
        }
        return object.toString();
    }

    @Override
    public int calculateSize(String object) {
        return Integer.BYTES + object.length() * Character.BYTES;
    }
}
