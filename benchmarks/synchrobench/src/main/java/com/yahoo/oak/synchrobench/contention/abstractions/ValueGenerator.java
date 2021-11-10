/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

/**
 * Generates value and serialize them.
 * Extends 'OakSerializer' for Oak.
 * Extends 'Transcoder' for Memcached.
 * Extends 'BytesReader/BytesWriter' for Chronicle.
 */
public interface ValueGenerator extends
    OakSerializer<BenchValue>,
    BytesReader<BenchValue>, BytesWriter<BenchValue>,
    Transcoder<BenchValue> {

    /**
     * Generates a random value.
     * @param rnd a random generator.
     * @return a new BenchValue
     */
    BenchValue getNextValue(Random rnd);

    /**
     * Generates a random value using a temporary generator.
     * @return a new BenchValue
     */
    default BenchValue getNextValue() {
        return getNextValue(new Random());
    }

    /**
     * Modify the value in some way.
     * @param obj the value
     */
    void updateValue(BenchValue obj);

    /**
     * Modify the value in some way.
     * @param buffer the value's buffer.
     */
    void updateSerializedValue(OakScopedWriteBuffer buffer);

    /**
     * Consume the data in the value into the black hole.
     * @param obj the value
     * @param blackHole the black hole
     */
    void consumeValue(BenchValue obj, Blackhole blackHole);

    /**
     * Consume the data in the value into the black hole.
     * @param buffer the value's buffer
     * @param blackHole the black hole
     */
    void consumeSerializedValue(OakBuffer buffer, Blackhole blackHole);

    /**
     * Default implementation for Memcached's Transcoder.
     */
    default boolean asyncDecode(CachedData d) {
        return false;
    }

    /**
     * Default implementation for Memcached's Transcoder using Oak serializer.
     */
    default int getMaxSize() {
        return calculateSize(getNextValue());
    }
}
