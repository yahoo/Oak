/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import org.openjdk.jmh.infra.Blackhole;

public interface CompositionalMap {

    boolean getOak(BenchKey key, Blackhole blackhole);

    void putOak(BenchKey key, BenchValue value);

    boolean putIfAbsentOak(BenchKey key, BenchValue value);

    void removeOak(BenchKey key);

    boolean computeIfPresentOak(BenchKey key);

    void computeOak(BenchKey key);

    boolean ascendOak(BenchKey from, int length, Blackhole blackhole);

    boolean descendOak(BenchKey from, int length, Blackhole blackhole);

    void clear();

    int size();

    void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value);

    default float allocatedGB() {
        return Float.NaN;
    }

    default void printMemStats() {
    }
}
