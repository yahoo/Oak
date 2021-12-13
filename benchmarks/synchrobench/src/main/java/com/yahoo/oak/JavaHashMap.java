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
import java.util.concurrent.ConcurrentHashMap;

public class JavaHashMap extends BenchOnHeapMap {

    private ConcurrentHashMap<BenchKey, BenchValue> hashMap = new ConcurrentHashMap<>();

    public JavaHashMap(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
    }

    /** {@inheritDoc} **/
    @Override
    public void init() {
        hashMap = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} **/
    @Override
    public void close() {
        super.close();
        hashMap = null;
    }

    /** {@inheritDoc} **/
    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        // disregard from, it is left to be consistent with the API
        return iterate(map().entrySet().iterator(), length, blackhole);
    }

    /** {@inheritDoc} **/
    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        // impossible to descend unordered map
        return ascendOak(from, length, blackhole);
    }

    /** {@inheritDoc} **/
    @Override
    protected AbstractMap<BenchKey, BenchValue> map() {
        return hashMap;
    }
}
