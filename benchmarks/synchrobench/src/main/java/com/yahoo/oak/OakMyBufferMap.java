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

public class OakMyBufferMap<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private OakMap<MyBuffer, MyBuffer> oak;
    private OakMapBuilder<MyBuffer, MyBuffer> builder;
    private MyBuffer minKey;
    private NativeMemoryAllocator ma;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;

    public OakMyBufferMap() {
        ma = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        if (Parameters.confDetailedStats) {
            ma.collectStats();
        }
        minKey = new MyBuffer(Integer.BYTES);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        builder =
            new OakMapBuilder<MyBuffer, MyBuffer>(
                MyBuffer.DEFAULT_COMPARATOR, MyBuffer.DEFAULT_SERIALIZER, MyBuffer.DEFAULT_SERIALIZER, minKey)
                .setOrderedChunkMaxItems(OrderedChunk.ORDERED_CHUNK_MAX_ITEMS_DEFAULT)
                .setMemoryAllocator(ma);
        oak = builder.buildOrderedMap();
    }

    public long allocated() {
        return ma.allocated();
    }

    @Override
    public boolean getOak(K key) {
        if (Parameters.confZeroCopy) {
            return oak.zc().get(key) != null;
        }
        return oak.get(key) != null;
    }

    @Override
    public void putOak(K key, V value) {
        oak.zc().put(key, value); //ZC update is usually the required interface. 
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        return oak.zc().putIfAbsent(key, value);
    }

    @Override
    public void removeOak(K key) {
        if (Parameters.confZeroCopy) {
            oak.zc().remove(key);
        } else {
            oak.remove(key);
        }
    }

    @Override
    public boolean computeIfPresentOak(K key) {
        return false;
    }

    @Override
    public void computeOak(K key) {

    }

    @Override
    public boolean ascendOak(K from, int length) {
        OakMap<MyBuffer, MyBuffer> sub = oak.tailMap(from, true);

        boolean result = createAndScanView(sub, length);

        sub.close();

        return result;
    }

    @Override
    public boolean descendOak(K from, int length) {
        OakMap<MyBuffer, MyBuffer> desc = oak.descendingMap();
        OakMap<MyBuffer, MyBuffer> sub = desc.tailMap(from, true);

        boolean result = createAndScanView(sub, length);

        sub.close();
        desc.close();

        return result;
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
        oak.close();

        ma = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        if (Parameters.confDetailedStats) {
            ma.collectStats();
        }
        minKey = new MyBuffer(Integer.BYTES);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        builder =
            new OakMapBuilder<MyBuffer, MyBuffer>(
                MyBuffer.DEFAULT_COMPARATOR, MyBuffer.DEFAULT_SERIALIZER, MyBuffer.DEFAULT_SERIALIZER, minKey)
                .setOrderedChunkMaxItems(OrderedChunk.ORDERED_CHUNK_MAX_ITEMS_DEFAULT)
                .setMemoryAllocator(ma);
        oak = builder.buildOrderedMap();
    }

    @Override
    public int size() {
        return oak.size();
    }

    @Override
    public void putIfAbsentComputeIfPresentOak(K key, V value) {
        oak.zc().putIfAbsentComputeIfPresent(key, value, b -> b.putLong(1, ~b.getLong(1)));
    }

    @Override
    public void printMemStats() {
        NativeMemoryAllocator.Stats stats = ma.getStats();
        System.out.printf("\tReleased buffers: \t\t%d\n", stats.releasedBuffers);
        System.out.printf("\tReleased bytes: \t\t%d\n", stats.releasedBytes);
        System.out.printf("\tReclaimed buffers: \t\t%d\n", stats.reclaimedBuffers);
        System.out.printf("\tReclaimed bytes: \t\t%d\n", stats.reclaimedBytes);

    }
}
