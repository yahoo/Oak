package com.oath.oak.common.string;

import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;

public class OakStringSerializer implements OakSerializer<String> {

    @Override
    public void serialize(String object, ByteBuffer targetBuffer) {
        final int offset = targetBuffer.position();
        final int size = object.length();

        targetBuffer.putInt(offset, size);
        for (int i = 0; i < object.length(); i++) {
            char c = object.charAt(i);
            targetBuffer.putChar(offset + Integer.BYTES + i * Character.BYTES, c);
        }
    }

    @Override
    public String deserialize(ByteBuffer byteBuffer) {
        final int offset = byteBuffer.position();
        final int size = byteBuffer.getInt(offset);

        StringBuilder object = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = byteBuffer.getChar(offset + Integer.BYTES + i * Character.BYTES);
            object.append(c);
        }
        return object.toString();
    }

    @Override
    public int calculateSize(String object) {
        return Integer.BYTES + object.length() * Character.BYTES;
    }
}
