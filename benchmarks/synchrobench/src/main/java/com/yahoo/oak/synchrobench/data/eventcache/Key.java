/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.eventcache;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;

public class Key implements BenchKey {
    long key1;
    long key2;

    public Key() {
        this(0, 0);
    }

    public Key(long key1, long key2) {
        this.key1 = key1;
        this.key2 = key2;
    }

    public long getKey1() {
        return key1;
    }

    public long getKey2() {
        return key2;
    }

    @Override // for ConcurrentSkipListMap
    public int compareTo(Object inputObj) {
        Key o = (Key) inputObj;
        int ret = Long.compare(this.key1, o.key1);
        return ret != 0 ? ret : Long.compare(this.key2, o.key2);
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Key)) {
            return false;
        }
        final Key other = (Key) object;
        return this.key1 == other.key1 && this.key2 == other.key2;
    }

    @Override
    public int hashCode() {
        int hashCode = 1000003;
        hashCode ^= Long.hashCode(key2);
        hashCode *= 1000003;
        hashCode ^= Long.hashCode(key1);
        return hashCode;
    }

    @Override
    public String toString() {
        return String.format("%d,%d", key1, key2);
    }
}
