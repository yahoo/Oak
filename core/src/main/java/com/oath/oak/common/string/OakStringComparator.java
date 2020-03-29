package com.oath.oak.common.string;

import com.oath.oak.OakComparator;

import java.nio.ByteBuffer;

public class OakStringComparator implements OakComparator<String> {

    @Override
    public int compareKeys(String key1, String key2) {
        return key1.compareTo(key2);
    }

    @Override
    public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
        final int offset1 = serializedKey1.position();
        final int offset2 = serializedKey2.position();

        final int size1 = serializedKey1.getInt(offset1);
        final int size2 = serializedKey2.getInt(offset2);
        final int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            char c1 = serializedKey1.getChar(offset1 + Integer.BYTES + i * Character.BYTES);
            char c2 = serializedKey2.getChar(offset2 + Integer.BYTES + i * Character.BYTES);
            int compare = Character.compare(c1, c2);
            if (compare != 0) {
                return compare;
            }
        }

        return size1 - size2;
    }

    @Override
    public int compareKeyAndSerializedKey(String key, ByteBuffer serializedKey) {
        final int offset = serializedKey.position();

        final int size1 = key.length();
        final int size2 = serializedKey.getInt(offset);
        final int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            char c1 = key.charAt(i);
            char c2 = serializedKey.getChar(offset + Integer.BYTES + i * Character.BYTES);
            int compare = Character.compare(c1, c2);
            if (compare != 0) {
                return compare;
            }
        }

        return size1 - size2;
    }
}
