/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.doublenum;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;

public class OakDoubleComparator implements OakComparator<Double> {
    @Override
    public int compareKeys(Double key1, Double key2) {
        return Double.compare(key1, key2);
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
        return Double.compare(serializedKey1.getDouble(0), serializedKey2.getDouble(0));
    }

    @Override
    public int compareKeyAndSerializedKey(Double key, OakScopedReadBuffer serializedKey) {
        return Double.compare(key, serializedKey.getDouble(0));
    }
}
