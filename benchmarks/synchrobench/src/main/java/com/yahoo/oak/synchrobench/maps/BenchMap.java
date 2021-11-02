/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.maps;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalMap;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Iterator;
import java.util.Map;

/**
 * Implements common logic for all 'CompositionalMap' implementations.
 */
public abstract class BenchMap implements CompositionalMap {
    protected static final long KB = 1024L;
    protected static final long GB = KB * KB * KB;

    protected final KeyGenerator keyGen;
    protected final ValueGenerator valueGen;

    public BenchMap(KeyGenerator keyGen, ValueGenerator valueGen) {
        this.keyGen = keyGen;
        this.valueGen = valueGen;
    }

    protected boolean iterate(Iterator<Map.Entry<BenchKey, BenchValue>> iter, int length, Blackhole blackhole) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            Map.Entry<BenchKey, BenchValue> entry = iter.next();
            if (Parameters.confConsumeKeys && blackhole != null) {
                keyGen.consumeKey(entry.getKey(), blackhole);
            }
            if (Parameters.confConsumeValues && blackhole != null) {
                valueGen.consumeValue(entry.getValue(), blackhole);
            }
        }
        return i == length;
    }
}
