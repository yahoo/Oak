/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

//
//import java.util.*;
//import java.util.concurrent.ConcurrentNavigableMap;
//import java.util.function.BiFunction;
//import java.util.function.Consumer;
//import java.util.function.Function;
//
///**
// * A concurrent map implementation which supports off-heap memory.
// */
import java.util.concurrent.ConcurrentHashMap;
public class OakHashMap<K, V> extends AbstractMap<K, V> implements AutoCloseable,ConcurrentMap<K,V>, Serializable {

    private final InternalOakHashMap<K, V> internalOakHashMap;
    private final MemoryManager valuesMemoryManager;
    private final MemoryManager keysMemoryManager;

    public OakHashMap(MemoryManager kMM, MemoryManager vMM) {
        this.keysMemoryManager = kMM;
        this.valuesMemoryManager = vMM;
        this.internalOakHashMap = new InternalOakHashMap<K,V>(this.keysMemoryManager,this.valuesMemoryManager);
    }


    @Override
    public void close() throws Exception {
        this.internalOakHashMap.close();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("Not implemented yet");
        return null;
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        Result res=this.internalOakHashMap.get((K)key);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {

    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException("Not implemented yet");
        return false;
    }

    @Override
    public V replace(K key, V value)
    {
        throw new UnsupportedOperationException("Not implemented yet");
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException("Not implemented yet");
        return false;
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException("Not implemented yet");
        return null;
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException("Not implemented yet");
        return null;
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException("Not implemented yet");
        return null;
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException("Not implemented yet");
        return null;
    }

    @Override
    public V putIfAbsent(K key, V value)
    {
        throw new UnsupportedOperationException("Not implemented yet");
        return null;
    }

    private void checkKey(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
//        if (!inBounds(key)) {
//            throw new IllegalArgumentException("The key is out of map bounds");
//        }
    }

}
