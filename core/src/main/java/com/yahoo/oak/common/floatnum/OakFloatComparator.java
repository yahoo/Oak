/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.floatnum;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;

public class OakFloatComparator implements OakComparator<Float> {
    @Override
    public int compareKeys(Float key1, Float key2) {
        return Float.compare(key1, key2);
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
        return Float.compare(serializedKey1.getFloat(0), serializedKey2.getFloat(0));
    }

    @Override
    public int compareKeyAndSerializedKey(Float key, OakScopedReadBuffer serializedKey) {
        return Float.compare(key, serializedKey.getFloat(0));
    }
}
