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
import com.yahoo.oak.synchrobench.maps.BenchOakMap;
import org.openjdk.jmh.infra.Blackhole;

public class OakBenchMap extends BenchOakMap {
    private NativeMemoryAllocator ma;
    private OakMap<BenchKey, BenchValue> oak;

    public OakBenchMap(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
    }

    @Override
    protected void build() {
        ma = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        if (Parameters.confDetailedStats) {
            ma.collectStats();
        }
        OakMapBuilder<BenchKey, BenchValue> builder = new OakMapBuilder<>(keyGen, keyGen, valueGen, minKey)
            .setOrderedChunkMaxItems(OrderedChunk.ORDERED_CHUNK_MAX_ITEMS_DEFAULT)
            .setMemoryAllocator(ma);
        oak = builder.buildOrderedMap();
    }

    @Override
    protected ConcurrentZCMap<BenchKey, BenchValue> map() {
        return oak;
    }

    @Override
    protected ZeroCopyMap<BenchKey, BenchValue> zc() {
        return oak.zc();
    }

    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        OakMap<BenchKey, BenchValue> sub = oak.tailMap(from, true);

        boolean result = createAndScanView(sub, length, blackhole);

        sub.close();

        return result;
    }

    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        OakMap<BenchKey, BenchValue> desc = oak.descendingMap();
        OakMap<BenchKey, BenchValue> sub = desc.tailMap(from, true);

        boolean result = createAndScanView(sub, length, blackhole);

        sub.close();
        desc.close();

        return result;
    }

    public void printMemStats() {
        NativeMemoryAllocator.Stats stats = ma.getStats();
        System.out.printf("\tReleased buffers: \t\t%d\n", stats.releasedBuffers);
        System.out.printf("\tReleased bytes: \t\t%d\n", stats.releasedBytes);
        System.out.printf("\tReclaimed buffers: \t\t%d\n", stats.reclaimedBuffers);
        System.out.printf("\tReclaimed bytes: \t\t%d\n", stats.reclaimedBytes);
    }
}
