/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;


import java.util.Comparator;

public interface OakComparator<K> extends Comparator<K> {
    default int compare(K key1, K key2) {
        return compareKeys(key1, key2);
    }

    int compareKeys(K key1, K key2);

    int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2);

    int compareKeyAndSerializedKey(K key, OakReadBuffer serializedKey);
}
