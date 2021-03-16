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
    public void serialize(final String object, final OakScopedWriteBuffer targetBuffer) {
        final int size = object.length();

        targetBuffer.putInt(0, size);
        for (int i = 0; i < object.length(); i++) {
            final char c = object.charAt(i);
            final int index = calculateIndex(i);
            targetBuffer.putChar(index, c);
        }
    }

    @Override
    public String deserialize(final OakScopedReadBuffer byteBuffer) {
        final int size = byteBuffer.getInt(0);

        final StringBuilder object = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            final int index = calculateIndex(i);
            final char c = byteBuffer.getChar(index);
            object.append(c);
        }
        return object.toString();
    }

    @Override
    public int calculateSize(final String object) {
        return calculateIndex(object.length());
    }

    protected int calculateIndex(final int i) {
        return Integer.BYTES + (i * Character.BYTES);
    }
}
