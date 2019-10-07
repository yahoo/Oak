package com.oath.oak.synchrobench.contention.abstractions;

public interface CompositionalOakMap<K, V> {

    boolean getOak(K key);

    void putOak(K key, V value);

    boolean putIfAbsentOak(K key, V value);

    void removeOak(K key);

    boolean computeIfPresentOak(K key);

    void computeOak(K key);

    boolean ascendOak(K from, int length);

    boolean descendOak(K from, int length);

    void clear();

    int size();

    void putIfAbsentComputeIfPresentOak(K key, V value);

}