/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import org.openjdk.jmh.infra.Blackhole;

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

    boolean getOak(BenchKey key, Blackhole blackhole);

    void putOak(BenchKey key, BenchValue value);

    boolean putIfAbsentOak(BenchKey key, BenchValue value);

    void removeOak(BenchKey key);

    boolean computeIfPresentOak(BenchKey key);

    void computeOak(BenchKey key);

    boolean ascendOak(BenchKey from, int length, Blackhole blackhole);

    boolean descendOak(BenchKey from, int length, Blackhole blackhole);

    int size();

    void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value);

    default float allocatedGB() {
        return Float.NaN;
    }

    default void printMemStats() {
    }
}
