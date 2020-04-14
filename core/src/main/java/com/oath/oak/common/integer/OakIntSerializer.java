package com.oath.oak.common.integer;

import com.oath.oak.OakReadBuffer;
import com.oath.oak.OakSerializer;
import com.oath.oak.OakWriteBuffer;

public class OakIntSerializer implements OakSerializer<Integer> {

    private final int size;

    public OakIntSerializer() {
        this(Integer.BYTES);
    }

    public OakIntSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(Integer value, OakWriteBuffer targetBuffer) {
        targetBuffer.putInt(0, value);
    }

    @Override
    public Integer deserialize(OakReadBuffer serializedValue) {
        return serializedValue.getInt(0);
    }

    @Override
    public int calculateSize(Integer value) {
        return size;
    }
}
