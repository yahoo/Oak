package com.oath.oak.synchrobench.maps;


import com.oath.oak.*;
import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.oath.oak.synchrobench.contention.benchmark.Parameters;

import java.nio.ByteBuffer;

public class OakMap<K, V> implements CompositionalOakMap<K, V> {
    private com.oath.oak.OakMap<MyBuffer, MyBuffer> oak;
    private OakMapBuilder<MyBuffer, MyBuffer> builder;
    private MyBuffer minKey;
    private OakBufferView oakView;
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
        oakView = oak.createBufferView();
    }

    @Override
    public boolean getOak(K key) {
        if (Parameters.bufferView) {
            OakBufferView<MyBuffer> bufferView = oak.createBufferView();
            return bufferView.get((MyBuffer) key) != null;
        }
        return oak.get((MyBuffer) key) != null;
    }

    @Override
    public void putOak(K key, V value) {
        oak.put((MyBuffer) key, (MyBuffer) value);
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        return oak.putIfAbsent((MyBuffer) key, (MyBuffer) value);
    }

    @Override
    public void removeOak(K key) {
        oak.remove((MyBuffer) key);
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
        com.oath.oak.OakMap<MyBuffer, MyBuffer> sub = oak.tailMap((MyBuffer) from, true);

        boolean result = createAndScanView(sub, length);

        sub.close();

        return result;
    }

    @Override
    public boolean descendOak(K from, int length) {
        com.oath.oak.OakMap<MyBuffer, MyBuffer> desc = oak.descendingMap();
        com.oath.oak.OakMap<MyBuffer, MyBuffer> sub = desc.tailMap((MyBuffer) from, true);

        boolean result = createAndScanView(sub, length);

        sub.close();
        desc.close();

        return result;
    }

    private boolean createAndScanView(com.oath.oak.OakMap<MyBuffer, MyBuffer> subMap, int length) {
        OakIterator<ByteBuffer> iter;
        Runnable closeView;
        if (Parameters.bufferView) {
            OakBufferView<MyBuffer> oakView = subMap.createBufferView();
            iter = oakView.keysIterator();
            closeView = () -> oakView.close();
        } else {
            OakTransformView<MyBuffer, ByteBuffer> transformView = subMap.createTransformView(e -> MyBufferOak.serializer.deserialize(e.getKey()).buffer);
            iter = transformView.keysIterator();
            closeView = () -> transformView.close();
        }

        boolean result = iterate(iter, length);
        try {
            closeView.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private boolean iterate(OakIterator<ByteBuffer> iter, int length) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    @Override
    public void clear() {
        try {
            oakView.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        oakView = oak.createBufferView();
    }

    @Override
    public int size() {
        return oak.entries();
    }

    public void printMemStats() {
        OakNativeMemoryAllocator.Stats stats = ma.getStats();
        System.out.printf("\tReleased buffers: \t\t%d\n", stats.releasedBuffers);
        System.out.printf("\tReleased bytes: \t\t%d\n", stats.releasedBytes);
        System.out.printf("\tReclaimed buffers: \t\t%d\n", stats.reclaimedBuffers);
        System.out.printf("\tReclaimed bytes: \t\t%d\n", stats.reclaimedBytes);

    }
}
