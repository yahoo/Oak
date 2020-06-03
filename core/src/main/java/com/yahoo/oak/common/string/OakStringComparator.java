/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.string;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;

public class OakStringComparator implements OakComparator<String> {

    @Override
    public int compareKeys(String key1, String key2) {
        return key1.compareTo(key2);
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
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
    public int compareKeyAndSerializedKey(String key, OakScopedReadBuffer serializedKey) {
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
