/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.buffer;

import com.yahoo.oak.common.MurmurHash3;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;

import java.nio.ByteBuffer;

/**
 * Buffer key or value. It has a predefined capacity and contains a buffer with that capacity.
 */
public class KeyValueBuffer implements BenchKey, BenchValue {
    public final int capacity;
    public final ByteBuffer buffer;

    public KeyValueBuffer(Integer capacity) {
        this.capacity = capacity;
        this.buffer = ByteBuffer.allocate(capacity);
    }

    /** {@inheritDoc} **/
    @Override
    public int compareTo(Object o) {
        return KeyValueGenerator.compareBuffers(this, (KeyValueBuffer) o);
    }

    /** {@inheritDoc} **/
    @Override
    public int hashCode() {
        // defined to satisfy fair comparison with ConcurrentHashMap
        //TODO: apply hash on exact number of data written in the buffer,
        //TODO: as the majority of the capacity space is just zeros
        return MurmurHash3.murmurhash32(buffer.array(), 0, capacity, 0);
    }
}
