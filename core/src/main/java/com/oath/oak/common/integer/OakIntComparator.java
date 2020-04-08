package com.oath.oak.common.integer;

import com.oath.oak.OakComparator;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class OakIntComparator implements OakComparator<Integer> {
    @Override
    public int compareKeys(Integer key1, Integer key2) {
        return Integer.compare(key1, key2);
    }

    @Override
    public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
        return compareSerializedInteger(serializedKey1, serializedKey2);
    }

    @Override
    public int compareKeyAndSerializedKey(Integer key, ByteBuffer serializedKey) {
        int int1 = serializedKey.getInt(serializedKey.position());
        return Integer.compare(key, int1);
    }

    public static int compareSerializedInteger(ByteBuffer bb1, ByteBuffer bb2) {
        int int1 = bb1.getInt(bb1.position());
        int int2 = bb2.getInt(bb2.position());
        return Integer.compare(int1, int2);
    }

    public static Comparator<ByteBuffer> getByteBufferIntComparator() {
        return new Comparator<ByteBuffer>() {
            @Override
            public int compare(ByteBuffer bb1, ByteBuffer bb2) {
                return compareSerializedInteger(bb1, bb2);
            }
        };
    }
}
