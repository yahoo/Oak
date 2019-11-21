package com.oath.oak;

import java.nio.ByteBuffer;

public class StringSerializer implements OakSerializer<String> {

    @Override
    public void serialize(String object, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), object.length());
        for (int i = 0; i < object.length(); i++) {
            char c = object.charAt(i);
            targetBuffer.putChar(Integer.BYTES + targetBuffer.position() + i * Character.BYTES, c);
        }
    }

    @Override
    public String deserialize(ByteBuffer byteBuffer) {
        int size = byteBuffer.getInt(byteBuffer.position());
        StringBuilder object = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = byteBuffer.getChar(Integer.BYTES + byteBuffer.position() + i * Character.BYTES);
            object.append(c);
        }
        return object.toString();
    }

    @Override
    public int calculateSize(String object) {
        return Integer.BYTES + object.length() * Character.BYTES;
    }

    // hash function from serialized version of the object to an integer
    public int serializedHash(ByteBuffer byteBuffer) {
        int size = byteBuffer.getInt(byteBuffer.position());
        int cnt = Math.min(size,100);
        int hash = 0;
        for (int i = 0; i < cnt; i++) {
            char c = byteBuffer.getChar(Integer.BYTES + byteBuffer.position() + i * Character.BYTES);
            hash+=((int)c);
        }
        return hash;
    }

    // hash function from a key to an integer
    public int hash(String object) {
        int l = object.length();
        int cnt = Math.min(l,100);
        int hash = 0;
        for (int i = 0; i < cnt; i++) {
            char c = object.charAt(i);
            hash+=((int)c);
        }
        return hash;
    }
}