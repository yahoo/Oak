package com.oath.oak.common.integer;

import com.oath.oak.OakComparator;
import com.oath.oak.OakScopedReadBuffer;

public class OakIntComparator implements OakComparator<Integer> {
    @Override
    public int compareKeys(Integer key1, Integer key2) {
        return Integer.compare(key1, key2);
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
        return Integer.compare(serializedKey1.getInt(0), serializedKey2.getInt(0));
    }

    @Override
    public int compareKeyAndSerializedKey(Integer key, OakScopedReadBuffer serializedKey) {
        return Integer.compare(key, serializedKey.getInt(0));
    }
}
