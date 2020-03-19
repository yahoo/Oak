/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;


import java.nio.ByteBuffer;

class StringComparator implements OakComparator<String>{

    @Override
    public int compareKeys(String key1, String key2) {
        return key1.compareTo(key2);
    }

    @Override
    public int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2) {

        int size1 = serializedKey1.getInt(0);
        int size2 = serializedKey2.getInt(0);

        int it=0;
        while (it < size1 && it < size2) {
            char c1 = serializedKey1.getChar(Integer.BYTES + it*Character.BYTES);
            char c2 = serializedKey2.getChar(Integer.BYTES + it*Character.BYTES);
            int compare = Character.compare(c1, c2);
            if (compare != 0) {
                return compare;
            }
            it++;
        }

        if (it == size1 && it == size2) {
            return 0;
        } else if (it == size1) {
            return -1;
        } else
            return 1;
    }

    @Override
    public int compareKeyAndSerializedKey(String key, OakReadBuffer serializedKey) {
        int size1 = key.length();
        int size2 = serializedKey.getInt(0);

        int it=0;
        while (it < size1 && it < size2) {
            char c1 = key.charAt(it);
            char c2 = serializedKey.getChar(Integer.BYTES + it * Character.BYTES);
            int compare = Character.compare(c1, c2);
            if (compare != 0) {
                return compare;
            }
            it++;
        }

        if (it == size1 && it == size2) {
            return 0;
        } else if (it == size1) {
            return -1;
        } else
            return 1;
    }

}
