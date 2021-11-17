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
 * A generic benchmark worker.
 * Is contains the common parts of all parallel workers that are used during the benchmark.
 */
public abstract class BenchWorker implements Runnable {

    /**
     * The instance of the running benchmark and key/value generators.
     */
    CompositionalMap bench;
    KeyGenerator keyGen;
    ValueGenerator valueGen;

    /**
     * A random generator for generating the values.
     */
    final Random valueRand;

    /**
     * A stream of integer that is used to generate the keys.
     */
    final PrimitiveIterator.OfInt keyIndexIterator;

    /**
     * Stores exceptions to later reported by the benchmark.
     */
    protected Exception error = null;

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

    /**
     * @return a new random or increasing integer stream according to the benchmark configuration.
     */
    private IntStream generateIntStream() {
        if (Parameters.isRandomKeyDistribution()) {
            return new Random().ints(0, Parameters.confRange);
        } else {
            return IncreasingKeyIndex.ints(Parameters.confRange);
        }
    }

    /**
     * @param e the exception to be reported
     */
    void reportError(Exception e) {
        error = e;
    }

    /**
     * If en error was reported, the reported exception will be thrown.
     * Otherwise, nothing should happen.
     */
    void getError() throws Exception {
        if (error != null) {
            throw error;
        }
    }

    /**
     * @return a new randomly generated key
     */
    protected BenchKey nextKey() {
        return keyGen.getNextKey(keyIndexIterator.next());
    }

    /**
     * @return a new randomly generated value
     */
    protected BenchValue nextValue() {
        return valueGen.getNextValue(valueRand);
    }
}
