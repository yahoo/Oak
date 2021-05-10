/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.AbstractSet;
import java.util.Set;
import java.util.Iterator;
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

public class OakHashMap<K, V> extends AbstractMap<K, V> implements AutoCloseable,ConcurrentMap<K,V>, Serializable {

    private final InternalOakHashMap<K, V> internalOakHashMap;
    private final MemoryManager valuesMemoryManager;
    private final MemoryManager keysMemoryManager;
    private final OakTransformer<K> keyDeserializeTransformer;
    private final OakTransformer<V> valueDeserializeTransformer;
    private final Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>,
            Map.Entry<K, V>> entryDeserializeTransformer;
    private final OakHash<K> hashFunction;
    public OakHashMap(MemoryManager kMM,
                      MemoryManager vMM,
                      OakTransformer<K> keyDeserializeTransformer,
                      OakTransformer<V> valueDeserializeTransformer,
                      Function<Entry<OakScopedReadBuffer, OakScopedReadBuffer>,
                              Entry<K, V>> entryDeserializeTransformer,
                      OakHash<K> hashFunction)
    {
        this.keysMemoryManager = kMM;
        this.valuesMemoryManager = vMM;
        this.keyDeserializeTransformer = keyDeserializeTransformer;
        this.valueDeserializeTransformer = valueDeserializeTransformer;
        this.entryDeserializeTransformer = entryDeserializeTransformer;
        this.hashFunction = hashFunction;
        this.internalOakHashMap = new InternalOakHashMap<K,V>(this.keysMemoryManager,this.valuesMemoryManager);
    }



    //Autocloseable
    /**
     * Close and release the map and all the memory that is used by it.
     * The user should ensure that there are no concurrent operations
     * that are undergoing at the time this method is called.
     * Failing to do so will result in an undefined behaviour.
     */
    @Override
    public void close() throws Exception {
        this.internalOakHashMap.close();
    }

    @Override
    public void clear()
    {
        this.internalOakHashMap.clear();
    }

    //get values
    /**
     * Returns a deserialized copy of the value to which the specified key is
     * mapped, or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with that key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException     if the specified key is null
     */
    public V get(Object key) {
        checkKey((K) key);
        return this.internalOakHashMap.getValueTransformation((K) key, valueDeserializeTransformer);
    }


    //insert values api
    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException     if any of the arguments are null
     */
    @Override
    public V put(K key, V value)
    {
        checkKey(key);
        return (V) this.internalOakHashMap.put(key, value, valueDeserializeTransformer).value;
    }

    /**
     * If the specified key is not already associated
     * with a value, associate it with the given value.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return {@code null} if there was no mapping for the key
     * @throws NullPointerException     if the specified key or value is null
     */
    public V putIfAbsent(K key, V value) {
        checkKey(key);
        if (value == null) {
            throw new NullPointerException();
        }
        return (V) this.internalOakHashMap.putIfAbsent(key, value, valueDeserializeTransformer).value;
    }

    //Modifying api
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object key, Object value) {
        checkKey((K) key);
        return (value != null) && (this.internalOakHashMap.remove((K) key, (V) value,
                valueDeserializeTransformer).operationResult == ValueUtils.ValueResult.TRUE);
    }


    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException     if the specified key or value is null
     */
    @Override
    public V replace(K key, V value) {
        checkKey(key);
        if (value == null) {
            throw new NullPointerException();
        }
        return this.internalOakHashMap.replace(key, value, valueDeserializeTransformer);
    }



    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException     if any of the arguments are null
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkKey(key);
        if (oldValue == null || newValue == null) {
            throw new NullPointerException();
        }
        return this.internalOakHashMap.replace(key, oldValue, newValue, valueDeserializeTransformer);
    }



    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        if(function==null)
        {
            throw new NullPointerException();
        }
         this.internalOakHashMap.replaceAll(function, valueDeserializeTransformer);
    }

    @Override
    public int size()
    {
        return this.internalOakHashMap.size();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new OakHashMap.EntrySet<>(this);
    }

    @Override
    public Set<K> keySet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<V> values()
    {
        throw new UnsupportedOperationException();
    }
    values




    private Iterator<V> valuesIterator() {
        return this.internalOakHashMap.valuesTransformIterator(valueDeserializeTransformer);
    }

    private Iterator<K> keysIterator() {
        return this.internalOakHashMap.keysTransformIterator(keyDeserializeTransformer);
    }
//    @Override
//    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
//        throw new UnsupportedOperationException("Not implemented yet");
//    }





    private void checkKey(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
    }

    /**
     * Returns a {@link Iterator} of the mappings contained in this map in ascending key order.
     */
    private Iterator<Entry<K, V>> entriesIterator() {
        return this.internalOakHashMap.entriesTransformIterator(entryDeserializeTransformer);
    }


    static class EntrySet<K, V> extends AbstractSet<Entry<K, V>> {
        private final OakHashMap<K, V> m;

        EntrySet(OakHashMap<K, V> m) {
            this.m = m;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return m.entriesIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }



    static class ValueSet<K, V> extends AbstractSet<V> {
        private final OakHashMap<K, V> m;

        ValueSet(OakHashMap<K, V> m) {
            this.m = m;
        }

        @Override
        public Iterator< V> iterator() {
            return m.valuesIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }



    static class KeySet<K, V> extends AbstractSet<K> {
        private final OakHashMap<K, V> m;

        KeySet(OakHashMap<K,V> m) {
            this.m = m;
        }

        @Override
        public Iterator<K> iterator() {
            return m.keysIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

}
