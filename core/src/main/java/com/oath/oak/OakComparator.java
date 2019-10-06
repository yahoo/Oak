/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;


import java.nio.ByteBuffer;
import java.util.Comparator;

// IMPORTANT:
// (1) input ByteBuffer position might be any non-negative integer
// (2) input ByteBuffer position shouldn't be changed as a side effect of comparision
public interface OakComparator<K> extends Comparator<K> {
    default int compare(K key1, K key2) {
        return compareKeys(key1, key2);
    }

    int compareKeys(K key1, K key2);

    int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2);

    int compareKeyAndSerializedKey(K key, ByteBuffer serializedKey);
}
