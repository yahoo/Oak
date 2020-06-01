package com.oath.oak.common.string;

import com.oath.oak.OakScopedReadBuffer;
import com.oath.oak.OakScopedWriteBuffer;
import com.oath.oak.OakSerializer;

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
