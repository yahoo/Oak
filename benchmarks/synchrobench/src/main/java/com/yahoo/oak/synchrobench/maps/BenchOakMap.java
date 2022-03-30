/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.maps;

import com.yahoo.oak.ConcurrentZCMap;
import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakUnscopedBuffer;
import com.yahoo.oak.ZeroCopyMap;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Iterator;
import java.util.Map;

/**
 * Implements common logic for both OakMap and OakHash.
 */
public abstract class BenchOakMap extends BenchMap {
    protected static final long OAK_MAX_OFF_MEMORY = 256 * GB;

    protected final BenchKey minKey;

    public BenchOakMap(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
        this.minKey = keyGen.getMinKey();
    }

    protected abstract ConcurrentZCMap<BenchKey, BenchValue> map();
    protected abstract ZeroCopyMap<BenchKey, BenchValue> zc();

    /** {@inheritDoc} **/
    @Override
    public void close() {
        map().close();
    }

    /** {@inheritDoc} **/
    @Override
    public boolean getOak(BenchKey key, Blackhole blackhole) {
        if (Parameters.confZeroCopy) {
            OakBuffer val = zc().get(key);
            if (val == null) {
                return false;
            }
            if (Parameters.confConsumeValues && blackhole != null) {
                valueGen.consumeSerializedValue(val, blackhole);
            }
        } else {
            BenchValue val = map().get(key);
            if (val == null) {
                return false;
            }
            if (Parameters.confConsumeValues && blackhole != null) {
                valueGen.consumeValue(val, blackhole);
            }
        }
        return true;
    }

    /** {@inheritDoc} **/
    @Override
    public void putOak(BenchKey key, BenchValue value) {
        zc().put(key, value);
    }

    /** {@inheritDoc} **/
    @Override
    public boolean putIfAbsentOak(BenchKey key, BenchValue value) {
        return zc().putIfAbsent(key, value);
    }

    /** {@inheritDoc} **/
    @Override
    public void removeOak(BenchKey key) {
        if (Parameters.confZeroCopy) {
            zc().remove(key);
        } else {
            map().remove(key);
        }
    }

    /** {@inheritDoc} **/
    @Override
    public void computeOak(BenchKey key) {
    }

    /** {@inheritDoc} **/
    @Override
    public boolean computeIfPresentOak(BenchKey key) {
        return false;
    }

    /** {@inheritDoc} **/
    @Override
    public void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value) {
        zc().putIfAbsentComputeIfPresent(key, value, valueGen::updateSerializedValue);
    }

    /** {@inheritDoc} **/
    @Override
    public int size() {
        return map().size();
    }

    /** {@inheritDoc} **/
    @Override
    public float nonHeapAllocatedGB() {
        return (float) map().memorySize() / (float) GB;
    }

    /**
     * Iterate over a map and consume the keys/values.
     * @param map map, or its subset
     * @param length the number of items to iterate over
     * @param blackhole used to consume the keys/values (if null, should not consume)
     * @return true if iterated successfully on "length" items.
     */
    protected boolean createAndScanView(ConcurrentZCMap<BenchKey, BenchValue> map, int length, Blackhole blackhole) {
        if (Parameters.confZeroCopy) {
            Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iter;
            if (Parameters.confStreamIteration) {
                iter = map.zc().entryStreamSet().iterator();
            } else {
                iter = map.zc().entrySet().iterator();
            }

            return iterateBuffer(iter, length, blackhole);
        } else {
            Iterator<Map.Entry<BenchKey, BenchValue>> iter = map.entrySet().iterator();
            return iterate(iter, length, blackhole);
        }
    }

    /**
     * Iterate over a map using ZC view and consume the keys/values.
     * @param iter the map's iterator
     * @param length the number of items to iterate over
     * @param blackhole used to consume the keys/values (if null, should not consume)
     * @return true if iterated successfully on "length" items.
     */
    protected boolean iterateBuffer(Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iter,
                                  int length, Blackhole blackhole) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> entry = iter.next();
            if (Parameters.confConsumeKeys && blackhole != null) {
                keyGen.consumeSerializedKey(entry.getKey(), blackhole);
            }
            if (Parameters.confConsumeValues && blackhole != null) {
                valueGen.consumeSerializedValue(entry.getValue(), blackhole);
            }
        }
        return i == length;
    }
}
