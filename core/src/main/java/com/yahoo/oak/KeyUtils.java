/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.ValueUtils.ValueResult;

class KeyUtils {

    
    /**
     * Compare a key with a serialized key that is pointed by a specific entry index, this function is used
     * when trying to compare any other key that is not minKey which is never deleted.
     *
     * @param key                    the key to compare.
     * @param serializedKey          the entry index to compare with.
     * @param cmp                    the compare function provided by the end user.
     * @return the comparison result
     */
    public static <K> int compareEntryKeyAndSerializedKey(K key, KeyBuffer serializedKey, OakComparator<K> cmp) 
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
    
    /**
     * Deserializes the key that is pointed by a specific entry index, this function is used
     * when trying to deserializes key on released chunk!
     *
     * @param serializedKey          the entry index to deserialize with.
     * @param deSerial               the serializer that is used.
     * @return the result
     */
    public static <K> K deSerializedKey(KeyBuffer serializedKey, OakSerializer<K> deSerial) 
            throws DeletedMemoryAccessException {
        if (serializedKey.s.lockRead() != ValueResult.TRUE) {
            throw new DeletedMemoryAccessException();
        }
        try {
            return deSerial.deserialize(serializedKey);
        } finally {
            serializedKey.s.unlockRead();
        }
    }
    
}
