/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.integer;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;

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
