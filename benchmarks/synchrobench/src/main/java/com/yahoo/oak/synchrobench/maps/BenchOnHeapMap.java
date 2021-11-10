/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.maps;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import org.openjdk.jmh.infra.Blackhole;

import java.util.AbstractMap;

/**
 * Implements common logic for JavaSkipListMap and JavaHashMap.
 */
public abstract class BenchOnHeapMap extends BenchMap {

    public BenchOnHeapMap(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
    }

    protected abstract AbstractMap<BenchKey, BenchValue> map();

    @Override
    public void close() {
        map().clear();
    }

    @Override
    public boolean getOak(BenchKey key, Blackhole blackhole) {
        BenchValue val = map().get(key);
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
        map().put(key, value);
    }

    @Override
    public boolean putIfAbsentOak(BenchKey key, BenchValue value) {
        return map().putIfAbsent(key, value) == null;
    }

    @Override
    public void removeOak(BenchKey key) {
        map().remove(key);
    }

    @Override
    public boolean computeIfPresentOak(BenchKey key) {
        return false;
    }

    @Override
    public void computeOak(BenchKey key) {
    }

    @Override
    public void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value) {
        map().merge(key, value, (old, v) -> {
            synchronized (old) {
                valueGen.updateValue(old);
            }
            return old;
        });
    }

    @Override
    public int size() {
        return map().size();
    }

}
