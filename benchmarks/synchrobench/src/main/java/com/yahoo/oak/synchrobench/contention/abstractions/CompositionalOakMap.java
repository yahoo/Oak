/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

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

    default void printMemStats() {
    }

}
