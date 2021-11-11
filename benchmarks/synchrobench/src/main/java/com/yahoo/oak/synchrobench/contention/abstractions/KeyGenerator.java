/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakSerializer;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

/**
 * Generates keys and serialize them.
 * Extends 'OakSerializer/OakComparator' for Oak.
 * Extends 'SizedReader/SizedWriter' for Chronicle.
 * Implements 'toString' for Memcached.
 */
public interface KeyGenerator extends
    OakSerializer<BenchKey>, OakComparator<BenchKey>,
    SizedReader<BenchKey>, SizedWriter<BenchKey> {

    /**
     * Generates a random key.
     * If rnd is null, generate a key that follows the previous one.
     * Note that the key distribution must preserve the expected cardinality (Parameters.confRange).
     * @param rnd a random generator.
     * @param range the cardinality of the keys.
     * @param prev the previously inserted key.
     * @return a new BenchKey
     */
    BenchKey getNextKey(Random rnd, int range, BenchKey prev);

    /**
     * @return the minimal key.
     */
    BenchKey getMinKey();

    /**
     * Consume the data in the key into the black hole.
     * @param obj the key
     * @param blackHole the black hole
     */
    void consumeKey(BenchKey obj, Blackhole blackHole);

    /**
     * Consume the data in the key into the black hole.
     * @param buffer the key's buffer
     * @param blackHole the black hole
     */
    void consumeSerializedKey(OakBuffer buffer, Blackhole blackHole);

    /**
     * Return a string that represents the key for Memcached.
     * The string must NOT contain spaces (Memcached constraint).
     * @param obj the key
     * @return a string that represents the key
     */
    default String toString(BenchKey obj) {
        return obj.toString();
    }

    /**
     * Default implementation for 'Chronicle' using Oak serializer.
     */
    @Override
    default long size(@NotNull BenchKey toWrite) {
        return calculateSize(toWrite);
    }
}
