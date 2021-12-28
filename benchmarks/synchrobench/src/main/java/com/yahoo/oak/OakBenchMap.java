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
import com.yahoo.oak.synchrobench.maps.BenchOakMap;
import org.openjdk.jmh.infra.Blackhole;

public class OakBenchMap extends BenchOakMap {
    private OakMap<BenchKey, BenchValue> oak;

    public OakBenchMap(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
    }

    /** {@inheritDoc} **/
    @Override
    public void init() {
        OakMapBuilder<BenchKey, BenchValue> builder = new OakMapBuilder<>(keyGen, keyGen, valueGen, minKey)
            .setOrderedChunkMaxItems(OrderedChunk.ORDERED_CHUNK_MAX_ITEMS_DEFAULT)
            .setMemoryCapacity(OAK_MAX_OFF_MEMORY);
        oak = builder.buildOrderedMap();
    }

    /** {@inheritDoc} **/
    @Override
    public void close() {
        super.close();
        oak = null;
    }

    /** {@inheritDoc} **/
    @Override
    protected ConcurrentZCMap<BenchKey, BenchValue> map() {
        return oak;
    }

    /** {@inheritDoc} **/
    @Override
    protected ZeroCopyMap<BenchKey, BenchValue> zc() {
        return oak.zc();
    }

    /** {@inheritDoc} **/
    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        OakMap<BenchKey, BenchValue> sub = oak.tailMap(from, true);

        boolean result = createAndScanView(sub, length, blackhole);

        sub.close();

        return result;
    }

    /** {@inheritDoc} **/
    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        OakMap<BenchKey, BenchValue> desc = oak.descendingMap();
        OakMap<BenchKey, BenchValue> sub = desc.tailMap(from, true);

        boolean result = createAndScanView(sub, length, blackhole);

        sub.close();
        desc.close();

        return result;
    }
}
