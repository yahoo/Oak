package com.oath.oak.common.intbuffer;

import com.oath.oak.OakComparator;

import java.nio.ByteBuffer;

public class OakIntBufferComparator implements OakComparator<ByteBuffer> {

    private final int size;

    public OakIntBufferComparator(int size) {
        this.size = size;
    }

    @Override
    public int compareKeys(ByteBuffer buff1, ByteBuffer buff2) {
        return compare(buff1, 0, size, buff2, 0, size);
    }

    @Override
    public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
        return compare(serializedKey1, serializedKey1.position(), size, serializedKey2, serializedKey2.position(), size);
    }

    @Override
    public int compareKeyAndSerializedKey(ByteBuffer key, ByteBuffer serializedKey) {
        return compare(key, 0, size, serializedKey, serializedKey.position(), size);
    }

    public static int compare(ByteBuffer buff1, int pos1, int size1, ByteBuffer buff2, int pos2, int size2) {
        int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            int i1 = buff1.getInt(pos1 + Integer.BYTES * i);
            int i2 = buff2.getInt(pos2 + Integer.BYTES * i);
            int compare = Integer.compare(i1, i2);
            if (compare != 0) {
                return compare;
            }
        }

        return Integer.compare(size1, size2);
    }
}
