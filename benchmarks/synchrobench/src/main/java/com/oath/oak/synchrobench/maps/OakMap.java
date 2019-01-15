package com.oath.oak.synchrobench.maps;


import com.oath.oak.*;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.oath.oak.synchrobench.contention.benchmark.Parameters;

public class OakMap<K, V> implements CompositionalOakMap<K, V> {
    private com.oath.oak.OakMap<MyBuffer, MyBuffer> oak;
    private OakMapBuilder<MyBuffer, MyBuffer> builder;
    private MyBuffer minKey;
    private OakBufferView oakView;

    public OakMap() {
        minKey = new MyBuffer(Integer.BYTES);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        builder = new OakMapBuilder<MyBuffer, MyBuffer>()
                .setKeySerializer(MyBufferOak.serializer)
                .setValueSerializer(MyBufferOak.serializer)
                .setMinKey(minKey)
                .setComparator(MyBufferOak.keysComparator)
                .setChunkBytesPerItem(Parameters.keySize + Integer.BYTES)
                .setChunkMaxItems(Chunk.MAX_ITEMS_DEFAULT);
        oak = builder.build();
        oakView = oak.createBufferView();
    }

    @Override
    public boolean getOak(K key) {
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
        int i;
        try (com.oath.oak.OakMap<MyBuffer, MyBuffer> sub = oak.tailMap((MyBuffer) from, true)) {
            OakBufferView<MyBuffer> oakView = sub.createBufferView();
            i = 0;
            OakIterator<OakRBuffer> iter = oakView.keysIterator();
            while (iter.hasNext() && i < length) {
                i++;
                iter.next();
            }
            try {
                oakView.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return i == length;
    }

    @Override
    public boolean descendOak(K from, int length) {
        com.oath.oak.OakMap<MyBuffer, MyBuffer> desc = oak.descendingMap();
        com.oath.oak.OakMap<MyBuffer, MyBuffer> sub = desc.tailMap((MyBuffer) from, true);
        OakBufferView<MyBuffer> oakView = sub.createBufferView();
        int i = 0;
        OakIterator<OakRBuffer> iter = oakView.keysIterator();
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        try {
            oakView.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        sub.close();
        desc.close();
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

        minKey = new MyBuffer(Integer.BYTES);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        builder = new OakMapBuilder<MyBuffer, MyBuffer>()
                .setKeySerializer(MyBufferOak.serializer)
                .setValueSerializer(MyBufferOak.serializer)
                .setMinKey(minKey)
                .setComparator(MyBufferOak.keysComparator)
                .setChunkBytesPerItem(Parameters.keySize + Integer.BYTES)
                .setChunkMaxItems(Chunk.MAX_ITEMS_DEFAULT);
        oak = builder.build();
        oakView = oak.createBufferView();
    }
}
