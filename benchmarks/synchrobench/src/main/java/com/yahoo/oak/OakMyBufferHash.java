/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.MyBuffer;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;

import java.util.Iterator;

public class OakMyBufferHash<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private OakHashMap<MyBuffer, MyBuffer> oakHash;
    private OakMapBuilder<MyBuffer, MyBuffer> builder;
    private MyBuffer minKey;
    private NativeMemoryAllocator ma;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;

    public OakMyBufferHash() {
        buildHash();
    }

    public long allocated() {
        return ma.allocated();
    }

    @Override
    public boolean getOak(K key) {
        if (Parameters.confZeroCopy) {
            return oakHash.zc().get(key) != null;
        }
        return oakHash.get(key) != null;
    }

    @Override
    public void putOak(K key, V value) {
        oakHash.zc().put(key, value);
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        return oakHash.zc().putIfAbsent(key, value);
    }

    @Override
    public void removeOak(K key) {
        if (Parameters.confZeroCopy) {
            oakHash.zc().remove(key);
        } else {
            oakHash.remove(key);
        }
    }

    @Override
    public boolean computeIfPresentOak(K key) {
        return false;
    }

    @Override
    public void computeOak(K key) {

    }

    // ALL ITERATORS ARE NOT YET SUPPORTED FOR HASH
    @Override
    public boolean ascendOak(K from, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean descendOak(K from, int length) {
        throw new UnsupportedOperationException();
    }

    public void buildHash() {
        ma = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        if (Parameters.confDetailedStats) {
            ma.collectStats();
        }
        minKey = new MyBuffer(Integer.BYTES);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        builder =
            new OakMapBuilder<MyBuffer, MyBuffer>(
                MyBuffer.DEFAULT_COMPARATOR, MyBuffer.DEFAULT_SERIALIZER, MyBuffer.DEFAULT_SERIALIZER, minKey)
                // 2048 * 8 = 16384 (2^14) entries in each chunk, each entry takes 24 bytes, each chunk requires
                // approximately 393216 bytes ~= 393KB ~= 0.4 MB
                .setHashChunkMaxItems(HashChunk.HASH_CHUNK_MAX_ITEMS_DEFAULT * 8)
                // 1024 * 16 = 16384 (2^14) preallocated chunks of the above size,
                // total on-heap memory requirement:
                // 2^28 * 24 = 6442450944 bytes ~= 6442451 KB ~= 6442 MB ~= 6.5 GB
                .setPreallocHashChunksNum(FirstLevelHashArray.HASH_CHUNK_NUM_DEFAULT * 16)
                .setMemoryAllocator(ma);
        // capable to keep 2^28 keys
        oakHash = builder.buildHashMap();
    }

    private boolean createAndScanView(OakMap<MyBuffer, MyBuffer> subMap, int length) {
        Iterator iter;
        if (Parameters.confZeroCopy) {
            if (Parameters.confStreamIteration) {
                iter = subMap.zc().entryStreamSet().iterator();
            } else {
                iter = subMap.zc().entrySet().iterator();
            }
        } else {
            iter = subMap.entrySet().iterator();
        }

        return iterate(iter, length);
    }

    private boolean iterate(Iterator iter, int length) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    @Override
    public void clear() {
        oakHash.close();
        buildHash();
    }

    @Override
    public int size() {
        return oakHash.size();
    }

    @Override
    public void putIfAbsentComputeIfPresentOak(K key, V value) {
        oakHash.zc().putIfAbsentComputeIfPresent(key, value, b -> b.putLong(1, ~b.getLong(1)));
    }

    public void printMemStats() {
        NativeMemoryAllocator.Stats stats = ma.getStats();
        System.out.printf("\tReleased buffers: \t\t%d\n", stats.releasedBuffers);
        System.out.printf("\tReleased bytes: \t\t%d\n", stats.releasedBytes);
        System.out.printf("\tReclaimed buffers: \t\t%d\n", stats.reclaimedBuffers);
        System.out.printf("\tReclaimed bytes: \t\t%d\n", stats.reclaimedBytes);

    }
}
