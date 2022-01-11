/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.eventcache;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;

/**
 * Event-cache key: with two "long" fields.
 */
public class Key implements BenchKey {
    long field1;
    long field2;

    public Key(long field1, long field2) {
        this.field1 = field1;
        this.field2 = field2;
    }

    public long getField1() {
        return field1;
    }

    public long getField2() {
        return field2;
    }

    /** {@inheritDoc} **/
    @Override
    public int compareTo(Object inputObj) {
        Key o = (Key) inputObj;
        int ret = Long.compare(this.field1, o.field1);
        return ret != 0 ? ret : Long.compare(this.field2, o.field2);
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
        return this.field1 == other.field1 && this.field2 == other.field2;
    }

    @Override
    public int hashCode() {
        int hashCode = 1000003;
        hashCode ^= Long.hashCode(field2);
        hashCode *= 1000003;
        hashCode ^= Long.hashCode(field1);
        return hashCode;
    }

    @Override
    public String toString() {
        return String.format("%d,%d", field1, field2);
    }
}
