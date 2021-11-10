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

import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Generic benchmark worker
 */
public abstract class BenchWorker implements Runnable {

    // The instance of the running benchmark
    CompositionalMap bench;
    KeyGenerator keyGen;
    ValueGenerator valueGen;

    final Random valueRand;
    final PrimitiveIterator.OfInt keyIndexIterator;

    public BenchWorker(
        CompositionalMap bench,
        KeyGenerator keyGen,
        ValueGenerator valueGen
    ) {
        this.bench = bench;
        this.keyGen = keyGen;
        this.valueGen = valueGen;
        this.valueRand = new Random();
        this.keyIndexIterator = generateIntStream().iterator();
    }

    private IntStream generateIntStream() {
        if (Parameters.isRandomKeyDistribution()) {
            return new Random().ints(0, Parameters.confRange);
        } else {
            return IncreasingKeyIndex.ints(Parameters.confRange);
        }
    }

    protected BenchKey nextKey() {
        return keyGen.getNextKey(keyIndexIterator.next());
    }

    protected BenchValue nextValue() {
        return valueGen.getNextValue(valueRand);
    }
}
