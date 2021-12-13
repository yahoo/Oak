/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalMap;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;


/**
 * A worker that is used to fill the map concurrently before the benchmarks starts.
 */
public class FillWorker extends BenchWorker {

    /**
     * The number of elements to fill.
     */
    final long size;

    /**
     * The number of operation performed by this worker.
     */
    long operations = 0;

    public FillWorker(
        CompositionalMap bench,
        KeyGenerator keyGen,
        ValueGenerator valueGen,
        long size
    ) {
        super(bench, keyGen, valueGen);
        this.size = size;
    }

    public long getOperations() {
        return operations;
    }

    @Override
    public void run() {
        try {
            fill();
        } catch (Exception e) {
            System.err.printf("Failed during initial fill: %s%n", e.getMessage());
            reportError(e);
        }
    }

    public void fill() {
        for (int i = 0; i < size; ) {
            BenchKey curKey = nextKey();
            BenchValue curValue = nextValue();

            if (bench.putIfAbsentOak(curKey, curValue)) {
                i++;
            }
            // counts all the putIfAbsent operations, not only the successful ones
            operations++;
        }
    }
}
