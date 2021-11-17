/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import org.openjdk.jmh.infra.Blackhole;

/**
 * To allow testing of a map implementation, the map should have a class that implement this interface.
 *
 * The benchmark infrastructure expect all the constructors of the `CompositionalMap` implementations to
 * accept two parameters: `KeyGenerator` and `ValueGenerator`.
 */
public interface CompositionalMap {

    /**
     * Initialize the map.
     * This might allocate resources, so it must be followed by `close()`.
     */
    void init();

    /**
     * Close the map and release all its resources.
     * Must call init to use it again.
     */
    void close();

    /**
     * Calls the map "get" operation.
     * See the map's doc for more details.
     * This call should consume the value if a black-hole is supplied.
     * @param key the fetched key
     * @param blackhole used to consume the key (if null, should not consume)
     * @return true if the map contains the key
     */
    boolean getOak(BenchKey key, Blackhole blackhole);

    /**
     * Calls the map "put" operation.
     * See the map's doc for more details.
     * @param key the inserted key
     * @param value the inserted value
     */
    void putOak(BenchKey key, BenchValue value);

    /**
     * Calls the map "putIfAbsent" operation.
     * See the map's doc for more details.
     * @param key the inserted key
     * @param value the inserted value
     * @return true if the value as inserted
     */
    boolean putIfAbsentOak(BenchKey key, BenchValue value);

    /**
     * Calls the map "putIfAbsentComputeIfPresentOak"/"merge" operation.
     * See the map's doc for more details.
     * @param key the inserted key
     * @param value the inserted value
     */
    void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value);

    /**
     * Calls the map "remove" operation.
     * See the map's doc for more details.
     * @param key the removed key
     */
    void removeOak(BenchKey key);

    boolean computeIfPresentOak(BenchKey key);

    void computeOak(BenchKey key);

    /**
     * Performs ascending iteration over the map items using map.entrySet() like operation.
     * See the map's doc for more details.
     * This call should consume the keys/values if a black-hole is supplied.
     * @param from the iteration begin point
     * @param length the number of items to iterate over
     * @param blackhole used to consume the keys/values (if null, should not consume)
     * @return true if iterated successfully on "length" items.
     */
    boolean ascendOak(BenchKey from, int length, Blackhole blackhole);

    /**
     * Performs descending iteration over the map items using map.entrySet() like operation.
     * See the map's doc for more details.
     * This call should consume the keys/values if a black-hole is supplied.
     * @param from the iteration begin point
     * @param length the number of items to iterate over
     * @param blackhole used to consume the keys/values (if null, should not consume)
     * @return true if iterated successfully on "length" items.
     */
    boolean descendOak(BenchKey from, int length, Blackhole blackhole);

    /**
     * @return the number of item in the map. See the map's doc for more details.
     */
    int size();

    /**
     * @return the non-heap (off-heap / native) allocated memory (in GB).
     */
    default float nonHeapAllocatedGB() {
        return Float.NaN;
    }

    /**
     * Prints memory statistics for debugging.
     */
    default void printMemStats() {
    }
}
