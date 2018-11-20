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
    public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {

        int size1 = serializedKey1.getInt(serializedKey1.position());
        int size2 = serializedKey2.getInt(serializedKey2.position());

        int it=0;
        while (it < size1 && it < size2) {
            char c1 = serializedKey1.getChar(Integer.BYTES + serializedKey1.position() + it*Character.BYTES);
            char c2 = serializedKey2.getChar(Integer.BYTES + serializedKey2.position() + it*Character.BYTES);
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
    public int compareSerializedKeyAndKey(ByteBuffer serializedKey, String key) {
        int size1 = serializedKey.getInt(serializedKey.position());
        int size2 = key.length();

        int it=0;
        while (it < size1 && it < size2) {
            char c1 = serializedKey.getChar(Integer.BYTES + serializedKey.position() + it*Character.BYTES);
            char c2 = key.charAt(it);
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
