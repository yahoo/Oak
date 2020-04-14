package com.oath.oak.common.integer;

import com.oath.oak.OakComparator;
import com.oath.oak.OakReadBuffer;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class OakIntComparator implements OakComparator<Integer> {
    @Override
    public int compareKeys(Integer key1, Integer key2) {
        return Integer.compare(key1, key2);
    }

    @Override
    public int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2) {
        return Integer.compare(serializedKey1.getInt(0), serializedKey2.getInt(0));
    }

    @Override
    public int compareKeyAndSerializedKey(Integer key, OakReadBuffer serializedKey) {
        return Integer.compare(key, serializedKey.getInt(0));
    }
}
