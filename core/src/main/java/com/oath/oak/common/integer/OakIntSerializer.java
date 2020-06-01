package com.oath.oak.common.integer;

import com.oath.oak.OakScopedReadBuffer;
import com.oath.oak.OakScopedWriteBuffer;
import com.oath.oak.OakSerializer;

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
