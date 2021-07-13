/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.MyBuffer;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalOakMap;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class JavaHashMap<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {

    private ConcurrentHashMap<MyBuffer, MyBuffer> hashMap = new ConcurrentHashMap<>();

    @Override
    public boolean getOak(K key) {
        return hashMap.get(key) != null;
    }

    @Override
    public void putOak(K key, V value) {
        hashMap.put(key, value);
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        return hashMap.putIfAbsent(key, value) == null;
    }

    @Override
    public void removeOak(K key) {
        hashMap.remove(key);
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

        hashMap.merge(key, value, (old, v) -> {
            synchronized (old) {
                old.buffer.putLong(1, ~old.buffer.getLong(1));
            }
            return old;
        });
    }

    @Override
    public boolean ascendOak(K from, int length) {
        // disregard from, it is left to be consistenrt with the API
        Iterator iter = hashMap.entrySet().iterator();
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    @Override
    public boolean descendOak(K from, int length) {
        // impossible to descend unordered map
        return ascendOak(from, length);
    }

    @Override
    public void clear() {
        hashMap = new ConcurrentHashMap<>();
    }

    @Override
    public int size() {
        return hashMap.size();
    }
}
