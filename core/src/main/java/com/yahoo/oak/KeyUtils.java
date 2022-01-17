/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.ValueUtils.ValueResult;

class KeyUtils {


    /* ==================== Methods for operating on existing off-heap keys ==================== */
    
    
    public static <K> int compareKeyAndSerializedKey(K key, KeyBuffer serializedKey, OakComparator<K> cmp) 
            throws DeletedMemoryAccessException {
        if (serializedKey.s.lockRead() != ValueResult.TRUE) {
            throw new DeletedMemoryAccessException();
        }
        try {
            return cmp.compareKeyAndSerializedKey(key, serializedKey);
        } finally {
            serializedKey.s.unlockRead();
        }
    }
    
    public static <K> int compareSerializedKeys(KeyBuffer serializedKey1,
            KeyBuffer serializedKey2, OakComparator<K> cmp) throws DeletedMemoryAccessException {
        if (    serializedKey1.s.lockRead() != ValueResult.TRUE ||
                serializedKey2.s.lockRead() != ValueResult.TRUE) {
            throw new DeletedMemoryAccessException();
        }
        try {
            return cmp.compareSerializedKeys(serializedKey1, serializedKey2);
        } finally {
            serializedKey1.s.unlockRead();
            serializedKey2.s.unlockRead();
        }
    }
}
