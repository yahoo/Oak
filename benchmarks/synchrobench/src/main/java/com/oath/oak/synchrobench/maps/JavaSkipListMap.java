package com.oath.oak.synchrobench.maps;

import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class JavaSkipListMap<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {

    private ConcurrentSkipListMap<MyBuffer, MyBuffer> skipListMap = new ConcurrentSkipListMap<>();

    @Override
    public boolean getOak(K key) {
        return skipListMap.get(key) != null;
    }

    @Override
    public void putOak(K key, V value) {
        skipListMap.put(key, value);
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        return skipListMap.putIfAbsent(key, value) == null;
    }

    @Override
    public void removeOak(K key) {
        skipListMap.remove(key);
    }

    @Override
    public boolean computeIfPresentOak(K key) {
        return false;
    }

    @Override
    public void computeOak(K key) {

    }

    @Override
    public void putIfAbsentComputeIfPresentOak(K key, V value) {

        skipListMap.merge(key, value, (old,v) -> {
            synchronized (old) {
                old.buffer.putLong(1, ~old.buffer.getLong(1));
            }
            return old;
        });
    }

    @Override
    public boolean ascendOak(K from, int length) {
        Iterator iter = skipListMap.tailMap(from, true).keySet().iterator();
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    @Override
    public boolean descendOak(K from, int length) {
        Iterator iter = skipListMap.descendingMap().tailMap(from, true).keySet().iterator();
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    @Override
    public void clear() {
        skipListMap = new ConcurrentSkipListMap<>();
    }

    @Override
    public int size() {
        return skipListMap.size();
    }
}
