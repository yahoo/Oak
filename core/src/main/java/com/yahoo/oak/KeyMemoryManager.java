/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


public interface KeyMemoryManager extends MemoryManager {

    <K> int compareKeyAndSerializedKey(K key, OakScopedReadBuffer serializedKey, OakComparator<K> cmp);

    <K> int compareSerializedKeys(OakScopedReadBuffer serializedKey1,
            OakScopedReadBuffer serializedKey2, OakComparator<K> cmp);

}
