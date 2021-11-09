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

import java.util.Random;


public class FillWorker implements Runnable {

    // The instance of the running benchmark
    CompositionalMap bench;
    KeyGenerator keyGen;
    ValueGenerator valueGen;

    BenchKey lastKey;

    final long size;
    final int range;

    // The random numbers
    final Random keyRand;
    final Random valueRand;

    long operations = 0;
    Exception error = null;

    public FillWorker(
        CompositionalMap bench,
        KeyGenerator keyGen,
        ValueGenerator valueGen,
        BenchKey lastKey,
        long size,
        int range
    ) {
        this.bench = bench;
        this.keyGen = keyGen;
        this.valueGen = valueGen;
        this.valueRand = new Random();
        this.lastKey = lastKey;
        this.size = size;
        this.range = range;

        // for the key distribution INCREASING we want to continue the increasing integers sequence,
        // started in the initial filling of the map
        // for the key distribution RANDOM the below value will be overwritten anyway
        this.keyRand = Parameters.isRandomKeyDistribution() ? this.valueRand : null;
    }

    public long getOperations() {
        return operations;
    }

    public BenchKey getLastKey() {
        return lastKey;
    }

    @Override
    public void run() {
        try {
            fill();
        } catch (Exception e) {
            System.err.printf("Failed during initial fill: %s%n", e.getMessage());
            error = e;
        }
    }

    public void fill() {
        for (long i = 0; i < size; ) {
            BenchKey curKey = keyGen.getNextKey(keyRand, range, lastKey);
            BenchValue curValue = valueGen.getNextValue(valueRand, range);

            if (bench.putIfAbsentOak(curKey, curValue)) {
                i++;
            }
            // counts all the putIfAbsent operations, not only the successful ones
            operations++;

            lastKey = curKey;
        }
    }
}
