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
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Required installation of Memcached on the machine.
 */
public class Memcached extends BenchMap {
    public static final int DAY = 60 * 60 * 24;

    final MemcachedClient mc;

    public Memcached(KeyGenerator keyGen, ValueGenerator valueGen) throws IOException {
        super(keyGen, valueGen);

        mc  = new MemcachedClient(
            new ConnectionFactoryBuilder()
                .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .build(),
            AddrUtil.getAddresses("localhost:11211")
        );
    }

    @Override
    public void init() {
        mc.flush();
    }

    @Override
    public void close() {
        init();
    }

    @Override
    public boolean getOak(BenchKey key, Blackhole blackhole) {
        BenchValue val = mc.get(keyGen.toString(key), valueGen);
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
        mc.set(keyGen.toString(key), DAY, value, valueGen);
    }

    @Override
    public boolean putIfAbsentOak(BenchKey key, BenchValue value) {
        mc.add(keyGen.toString(key), DAY, value, valueGen);
        return true;
    }

    @Override
    public void removeOak(BenchKey key) {
        mc.delete(keyGen.toString(key));
    }

    @Override
    public void computeOak(BenchKey key) {
        throw new UnsupportedOperationException("Memcached does not support generic updates.");
    }

    @Override
    public boolean computeIfPresentOak(BenchKey key) {
        throw new UnsupportedOperationException("Memcached does not support generic updates.");
    }

    @Override
    public void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value) {
        throw new UnsupportedOperationException("Memcached does not support generic updates.");
    }

    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        throw new UnsupportedOperationException("Memcached does not support iterations.");
    }

    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        throw new UnsupportedOperationException("Memcached does not support iterations.");
    }

    @Override
    public int size() {
        return Integer.parseInt(stats().get("total_items"));
    }

    @Override
    public float allocatedGB() {
        return (float) Long.parseLong(stats().get("bytes")) / (float) GB;
    }

    public Map<String, String> stats() {
        Optional<Map<String, String>> ret = mc.getStats().values().stream().findFirst();
        assert ret.isPresent();
        return ret.get();
    }
}
