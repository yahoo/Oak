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
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import com.yahoo.oak.synchrobench.maps.BenchMap;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

public class Chronicle extends BenchMap {
    private ChronicleMap<BenchKey, BenchValue> map = null;

    public Chronicle(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
    }

    @Override
    public void init() {
        this.map = ChronicleMapBuilder.of(BenchKey.class, BenchValue.class)
            .entries(Parameters.confSize * 2L)
            .keyMarshallers(keyGen, keyGen)
            .valueMarshallers(valueGen, valueGen)
            .constantKeySizeBySample(keyGen.getMinKey())
            .constantValueSizeBySample(valueGen.getNextValue(new Random(), Integer.MAX_VALUE))
            .putReturnsNull(true)
            .removeReturnsNull(true)
            .maxBloatFactor(2)
            .create();
    }

    @Override
    public void close() {
        map.clear();
        map.close();
        map = null;
    }

    @Override
    public boolean getOak(BenchKey key, Blackhole blackhole) {
        BenchValue val = map.get(key);
        if (val == null) {
            return false;
        }
        if (Parameters.confConsumeValues && blackhole != null) {
            valueGen.consumeValue(val, blackhole);
        }
        return true;
    }

    @Override
    public void putOak(BenchKey key, BenchValue value) {
        map.put(key, value);
    }

    @Override
    public boolean putIfAbsentOak(BenchKey key, BenchValue value) {
        return map.putIfAbsent(key, value) == null;
    }

    @Override
    public void removeOak(BenchKey key) {
        map.remove(key);
    }

    @Override
    public boolean computeIfPresentOak(BenchKey key) {
        return map.computeIfPresent(key, (ignoredKey, val) -> {
            valueGen.updateValue(val);
            return val;
        }) != null;
    }

    @Override
    public void computeOak(BenchKey key) {
        map.compute(key, (ignoredKey, val) -> {
            valueGen.updateValue(val);
            return val;
        });
    }

    @Override
    public void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value) {
    }

    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        return iterate(map.entrySet().iterator(), length, blackhole);
    }

    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        return ascendOak(from, length, blackhole);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public float allocatedGB() {
        return (float) map.offHeapMemoryUsed() / (float) GB;
    }
}
