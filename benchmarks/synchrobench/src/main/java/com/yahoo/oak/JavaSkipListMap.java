/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import com.yahoo.oak.synchrobench.maps.BenchOnHeapMap;
import org.openjdk.jmh.infra.Blackhole;

import java.util.AbstractMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class JavaSkipListMap extends BenchOnHeapMap {

    private ConcurrentSkipListMap<BenchKey, BenchValue> skipListMap = new ConcurrentSkipListMap<>();

    public JavaSkipListMap(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
    }

    @Override
    public void init() {
        skipListMap = new ConcurrentSkipListMap<>();
    }

    @Override
    public void close() {
        super.close();
        skipListMap = null;
    }

    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        return iterate(skipListMap.tailMap(from, true).entrySet().iterator(), length, blackhole);
    }

    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        return iterate(skipListMap.descendingMap().tailMap(from, true).entrySet().iterator(), length, blackhole);
    }

    @Override
    protected AbstractMap<BenchKey, BenchValue> map() {
        return skipListMap;
    }

    @Override
    public int size() {
        return skipListMap.size();
    }
}
