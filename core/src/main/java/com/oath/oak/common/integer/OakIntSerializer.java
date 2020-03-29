package com.oath.oak.common.integer;

import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;

public class OakIntSerializer implements OakSerializer<Integer> {

    private final int size;

    public OakIntSerializer() {
        this(Integer.BYTES);
    }

    public OakIntSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(Integer value, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), value);
    }

    @Override
    public Integer deserialize(ByteBuffer serializedValue) {
        return serializedValue.getInt(serializedValue.position());
    }

    @Override
    public int calculateSize(Integer value) {
        return size;
    }
}
