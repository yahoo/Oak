package com.oath.oak.common.string;

import com.oath.oak.OakComparator;
import com.oath.oak.OakReadBuffer;

public class OakStringComparator implements OakComparator<String> {

    @Override
    public int compareKeys(String key1, String key2) {
        return key1.compareTo(key2);
    }

    @Override
    public int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2) {
        final int size1 = serializedKey1.getInt(0);
        final int size2 = serializedKey2.getInt(0);
        final int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            char c1 = serializedKey1.getChar(Integer.BYTES + i * Character.BYTES);
            char c2 = serializedKey2.getChar(Integer.BYTES + i * Character.BYTES);
            int compare = Character.compare(c1, c2);
            if (compare != 0) {
                return compare;
            }
        }

        return size1 - size2;
    }

    @Override
    public int compareKeyAndSerializedKey(String key, OakReadBuffer serializedKey) {
        final int size1 = key.length();
        final int size2 = serializedKey.getInt(0);
        final int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            char c1 = key.charAt(i);
            char c2 = serializedKey.getChar(Integer.BYTES + i * Character.BYTES);
            int compare = Character.compare(c1, c2);
            if (compare != 0) {
                return compare;
            }
        }

        return size1 - size2;
    }
}
