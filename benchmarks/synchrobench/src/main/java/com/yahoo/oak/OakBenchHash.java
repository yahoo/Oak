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


public class OakBenchHash extends BenchOakMap {
    private OakHashMap<BenchKey, BenchValue> oakHash;

    public OakBenchHash(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);
    }

    /** {@inheritDoc} **/
    @Override
    public void init() {
        OakMapBuilder<BenchKey, BenchValue> builder = new OakMapBuilder<>(keyGen, keyGen, valueGen, minKey)
            // 2048 * 8 = 16384 (2^14) entries in each chunk, each entry takes 24 bytes, each chunk requires
            // approximately 393216 bytes ~= 393KB ~= 0.4 MB
            .setChunkMaxItems(Parameters.confSmallFootprint ? HashChunk.HASH_CHUNK_MAX_ITEMS_DEFAULT
                : HashChunk.HASH_CHUNK_MAX_ITEMS_DEFAULT * 8)
            // 1024 * 16 = 16384 (2^14) preallocated chunks of the above size,
            // total on-heap memory requirement:
            // 2^28 * 24 = 6442450944 bytes ~= 6442451 KB ~= 6442 MB ~= 6.5 GB
            .setPreallocHashChunksNum(Parameters.confSmallFootprint ? FirstLevelHashArray.HASH_CHUNK_NUM_DEFAULT
                : FirstLevelHashArray.HASH_CHUNK_NUM_DEFAULT * 16)
            .setMemoryCapacity(OAK_MAX_OFF_MEMORY);
        // capable to keep 2^28 keys
        oakHash = builder.buildHashMap();
    }

    /** {@inheritDoc} **/
    @Override
    public void close() {
        super.close();
        oakHash = null;
    }

    /** {@inheritDoc} **/
    @Override
    protected ConcurrentZCMap<BenchKey, BenchValue> map() {
        return oakHash;
    }

    /** {@inheritDoc} **/
    @Override
    protected ZeroCopyMap<BenchKey, BenchValue> zc() {
        return oakHash.zc();
    }

    /** {@inheritDoc} **/
    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        throw new UnsupportedOperationException("ALL ITERATORS ARE NOT YET SUPPORTED FOR HASH");
    }

    /** {@inheritDoc} **/
    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        throw new UnsupportedOperationException("ALL ITERATORS ARE NOT YET SUPPORTED FOR HASH");
    }
}
