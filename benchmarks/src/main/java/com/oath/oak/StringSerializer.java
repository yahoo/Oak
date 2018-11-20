/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

class StringSerializer implements OakSerializer<String> {

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
}