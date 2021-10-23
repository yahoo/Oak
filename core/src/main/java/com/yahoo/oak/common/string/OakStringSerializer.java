/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.string;

import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.common.MurmurHash3;

public class OakStringSerializer implements OakSerializer<String> {

    @Override
    public void serialize(String object, OakScopedWriteBuffer targetBuffer) {
        final int size = object.length();

        targetBuffer.putInt(0, size);
        int index = Integer.BYTES;
        for (int i = 0; i < object.length(); i++) {
            char c = object.charAt(i);
            targetBuffer.putChar(index, c);
            index += Character.BYTES;
        }
    }

    @Override
    public String deserialize(OakScopedReadBuffer byteBuffer) {
        final int size = byteBuffer.getInt(0);

        StringBuilder object = new StringBuilder(size);
        int index = Integer.BYTES;
        for (int i = 0; i < size; i++) {
            char c = byteBuffer.getChar(index);
            object.append(c);
            index += Character.BYTES;
        }
        return object.toString();
    }

    @Override
    public int calculateSize(String object) {
        return Integer.BYTES + object.length() * Character.BYTES;
    }

    @Override
    public int calculateHash(String object) {
        byte[] byteArray = object.getBytes();
        return MurmurHash3.murmurhash32(byteArray, 0, byteArray.length, 0);
    }
}
