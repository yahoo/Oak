package com.oath.oak.synchrobench.maps;


import com.oath.oak.Chunk;
import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.OakMapBuilder;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.oath.oak.synchrobench.contention.benchmark.Parameters;

import java.util.Iterator;

public class OakMap<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private com.oath.oak.OakMap<MyBuffer, MyBuffer> oak;
    private OakMapBuilder<MyBuffer, MyBuffer> builder;
    private MyBuffer minKey;
    private OakNativeMemoryAllocator ma;

    public OakMap() {
        ma = new OakNativeMemoryAllocator(Integer.MAX_VALUE);
        if (Parameters.detailedStats) {
            ma.collectStats();
        }
        minKey = new MyBuffer(Integer.BYTES);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        builder = new OakMapBuilder<MyBuffer, MyBuffer>()
                .setKeySerializer(MyBufferOak.serializer)
                .setValueSerializer(MyBufferOak.serializer)
                .setMinKey(minKey)
                .setComparator(MyBufferOak.keysComparator)
                .setChunkBytesPerItem(Parameters.keySize + Integer.BYTES)
                .setChunkMaxItems(Chunk.MAX_ITEMS_DEFAULT)
                .setMemoryAllocator(ma);
        oak = builder.build();
    }

    @Override
    public boolean getOak(K key) {
        if (Parameters.zeroCopy) {
            return oak.zc().get(key) != null;
        }
        return oak.get(key) != null;
    }

    @Override
    public void putOak(K key, V value) {
        oak.zc().put(key, value);
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        return oak.zc().putIfAbsent(key, value);
    }

    @Override
    public void removeOak(K key) {
        oak.remove(key);
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
        com.oath.oak.OakMap<MyBuffer, MyBuffer> sub = oak.tailMap(from, true);

        boolean result = createAndScanView(sub, length);

        sub.close();

        return result;
    }

    @Override
    public boolean descendOak(K from, int length) {
        com.oath.oak.OakMap<MyBuffer, MyBuffer> desc = oak.descendingMap();
        com.oath.oak.OakMap<MyBuffer, MyBuffer> sub = desc.tailMap(from, true);

        boolean result = createAndScanView(sub, length);

        sub.close();
        desc.close();

        return result;
    }

    private boolean createAndScanView(com.oath.oak.OakMap<MyBuffer, MyBuffer> subMap, int length) {
        Iterator iter;
        if (Parameters.zeroCopy) {
            iter = subMap.zc().keySet().iterator();
        } else {
            iter = subMap.keySet().iterator();
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

        ma = new OakNativeMemoryAllocator(Integer.MAX_VALUE);
        if (Parameters.detailedStats) {
            ma.collectStats();
        }
        minKey = new MyBuffer(Integer.BYTES);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        builder = new OakMapBuilder<MyBuffer, MyBuffer>()
                .setKeySerializer(MyBufferOak.serializer)
                .setValueSerializer(MyBufferOak.serializer)
                .setMinKey(minKey)
                .setComparator(MyBufferOak.keysComparator)
                .setChunkBytesPerItem(Parameters.keySize + Integer.BYTES)
                .setChunkMaxItems(Chunk.MAX_ITEMS_DEFAULT)
                .setMemoryAllocator(ma);
        oak = builder.build();
    }

    @Override
    public int size() {
        return oak.size();
    }

    public void printMemStats() {
        OakNativeMemoryAllocator.Stats stats = ma.getStats();
        System.out.printf("\tReleased buffers: \t\t%d\n", stats.releasedBuffers);
        System.out.printf("\tReleased bytes: \t\t%d\n", stats.releasedBytes);
        System.out.printf("\tReclaimed buffers: \t\t%d\n", stats.reclaimedBuffers);
        System.out.printf("\tReclaimed bytes: \t\t%d\n", stats.reclaimedBytes);

    }
}
