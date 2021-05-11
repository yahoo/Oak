/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A concurrent map implementation which supports off-heap memory.
 */
public class OakHashMap<K, V> extends AbstractMap<K, V> implements AutoCloseable, ConcurrentMap<K , V> {

    private final InternalOakHashMap<K , V> internalOakHashMap;
    /*
     * Memory manager cares for allocation, de-allocation and reuse of the internally pre-allocated
     * memory. There can be separate memory managing algorithms for keys and values.
     * */
    private final MemoryManager valuesMemoryManager;
    private final MemoryManager keysMemoryManager;
    private final OakTransformer<K> keyDeserializeTransformer;
    private final OakTransformer<V> valueDeserializeTransformer;
    private final Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>,
            Map.Entry<K, V>> entryDeserializeTransformer;
    private final OakComparator<K> comparator;
    private final OakHashFunction<K , ?> hashFunction;


    // internal constructor, to create OakHashMap use OakHashMapBuilder
    OakHashMap(OakSerializer<K> keySerializer,
               OakSerializer<V> valueSerializer,
               OakComparator<K> oakComparator,
               int chunkMaxItems,
               MemoryManager vMM,
               MemoryManager kMM, OakHashFunction hashFunction) {
        this.valuesMemoryManager = vMM;
        this.keysMemoryManager = kMM;
        this.comparator = oakComparator;
        this.keyDeserializeTransformer = keySerializer::deserialize;
        this.valueDeserializeTransformer = valueSerializer::deserialize;
        this.hashFunction = hashFunction;
        this.entryDeserializeTransformer = entry -> new AbstractMap.SimpleEntry<>(
                keySerializer.deserialize(entry.getKey()),
                valueSerializer.deserialize(entry.getValue()));
        this.internalOakHashMap = new InternalOakHashMap<>(vMM , kMM , keySerializer , valueSerializer , hashFunction);
    }

    /* ------ Map API methods ------ */

    /**
     * Returns the current number of key-value mappings in this HashMap.
     * Not supported for SubMaps.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    public int size() {
        return internalOakHashMap.size();
    }

    /**
     * Returns a deserialized copy of the value to which the specified key is
     * mapped, or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with that key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException     if the specified key is null
     */
    @Override
    public V get(Object key) {
        checkKey((K) key);
        return internalOakHashMap.getValueTransformation((K) key, valueDeserializeTransformer);
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     * Creates a copy of the value in the map.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with that key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException     if the specified key is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    @Override
    public V put(K key, V value) {
        checkKey(key);
        if (value == null) {
            throw new NullPointerException();
        }
        return internalOakHashMap.put(key, value, valueDeserializeTransformer);
    }

    /**
     * Removes the mapping for a key from this HashMap if it is present.
     *
     * @param key key whose mapping is to be removed from the map
     * @return the previous value associated with the provided key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException     if the specified key is null
     */
    @Override
    public V remove(Object key) {
        checkKey((K) key);
        return (V) internalOakHashMap.remove((K) key, null, valueDeserializeTransformer).value;
    }


    /* ------ ConcurrentMap API methods ------ */

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object key, Object value) {
        checkKey((K) key);
        Objects.requireNonNull(value);
        return  (internalOakHashMap.remove((K) key, (V) value,
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
        return internalOakHashMap.replace(key, value, valueDeserializeTransformer);
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

        return internalOakHashMap.replace(key, oldValue, newValue, valueDeserializeTransformer);
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
        return (V) internalOakHashMap.putIfAbsent(key, value, valueDeserializeTransformer).value;
    }





    /*-------------- SubMap --------------*/


    @Override
    public Set<K> keySet() {
        return new KeySet<>(this);
    }


    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet<>(this);
    }

    @Override
    public Collection<V> values() {
        return new Values<>(this);
    }

    /* ------ Zero-Copy API methods  ------ */

    public ZeroCopyMap<K, V> zc() {
        return new OakZeroCopyMap<>(this);
    }

    public MemoryManager getValuesMemoryManager() {
        return valuesMemoryManager;
    }

    public static class OakZeroCopyMap<K, V> implements ZeroCopyMap<K, V> {
        private OakHashMap<K, V> m;

        OakZeroCopyMap(OakHashMap<K, V> oakHashMap) {
            this.m = oakHashMap;
        }

        public void put(K key, V value) {
            m.checkKey(key);
            if (value == null) {
                throw new NullPointerException();
            }

            m.internalOakHashMap.put(key, value, null);
        }

        public OakUnscopedBuffer get(K key) {
            m.checkKey(key);

            return m.internalOakHashMap.get(key);
        }

        public boolean remove(K key) {
            m.checkKey(key);
            return m.internalOakHashMap.remove(key, null, null).operationResult == ValueUtils.ValueResult.TRUE;
        }

        public boolean putIfAbsent(K key, V value) {
            m.checkKey(key);
            if (value == null) {
                throw new NullPointerException();
            }

            return m.internalOakHashMap.putIfAbsent(key, value, null).operationResult == ValueUtils.ValueResult.TRUE;
        }

        public boolean computeIfPresent(K key, Consumer<OakScopedWriteBuffer> computer) {
            m.checkKey(key);
            if (computer == null) {
                throw new NullPointerException();
            }

            return m.internalOakHashMap.computeIfPresent(key, computer);
        }


        public boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakScopedWriteBuffer> computer) {
            m.checkKey(key);
            if (value == null || computer == null) {
                throw new IllegalArgumentException();
            }

            return m.internalOakHashMap.putIfAbsentComputeIfPresent(key, value, computer);
        }


        public Set<OakUnscopedBuffer> keySet() {
            return new KeyBufferSet<>(m);
        }

        public Collection<OakUnscopedBuffer> values() {
            return new ValueBuffers<>(m);
        }

        public Set<Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entrySet() {
            return new EntryBufferSet<>(m);
        }

        public Set<OakUnscopedBuffer> keyStreamSet() {
            return new KeyStreamBufferSet<>(m);
        }

        public Collection<OakUnscopedBuffer> valuesStream() {
            return new ValueStreamBuffers<>(m);
        }

        public Set<Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entryStreamSet() {
            return new EntryStreamBufferSet<>(m);
        }
    }


    /* ----------- Oak misc methods ----------- */

    /**
     * @return current off heap memory usage in bytes
     */
    public long memorySize() {
        return internalOakHashMap.memorySize();
    }

    /**
     * Close and release the map and all the memory that is used by it.
     * The user should ensure that there are no concurrent operations
     * that are undergoing at the time this method is called.
     * Failing to do so will result in an undefined behaviour.
     */
    @Override
    public void close() {
        internalOakHashMap.close();
    }


    /* ---------------- Private utility methods -------------- */

    private void checkKey(K key) {
        Objects.requireNonNull(key);
    }


    /**
     * Returns a {@link Iterator} of the values contained in this map
     * in ascending order of the corresponding keys.
     */
    private Iterator<V> valuesIterator() {
        return internalOakHashMap.valuesTransformIterator(valueDeserializeTransformer);
    }

    /**
     * Returns a {@link Iterator} of the mappings contained in this map in ascending key order.
     */
    private Iterator<Map.Entry<K, V>> entriesIterator() {
        return internalOakHashMap.entriesTransformIterator(entryDeserializeTransformer);
    }

    /**
     * Returns a {@link Iterator} of the keys contained in this map in ascending order.
     */
    private Iterator<K> keysIterator() {
        return internalOakHashMap.keysTransformIterator(keyDeserializeTransformer);
    }

    private Iterator<OakUnscopedBuffer> keysBufferIterator() {
        return internalOakHashMap.keysBufferViewIterator();
    }


    private Iterator<OakUnscopedBuffer> valuesBufferIterator() {
        return internalOakHashMap.valuesBufferViewIterator();
    }

    private Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesBufferIterator() {
        return internalOakHashMap.entriesBufferViewIterator();
    }

    private Iterator<OakUnscopedBuffer> keysStreamIterator() {
        return internalOakHashMap.keysStreamIterator();
    }


    private Iterator<OakUnscopedBuffer> valuesStreamIterator() {
        return internalOakHashMap.valuesStreamIterator();
    }

    private Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesStreamIterator() {
        return internalOakHashMap.entriesStreamIterator();
    }

    /* ---------------- TODO: Move methods below to their proper place as they are implemented -------------- */


    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return null;
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return null;
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return null;
    }

    /* ---------------- View Classes -------------- */

    static class KeySet<K> extends AbstractSet<K>  {

        private final OakHashMap<K, ?> m;

        KeySet(OakHashMap<K, ?> m) {
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

    static class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
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

    static final class Values<V> extends AbstractCollection<V> {

        private final OakHashMap<?, V> m;

        Values(OakHashMap<?, V> oakHashMap) {
            this.m = oakHashMap;
        }

        @Override
        public Iterator<V> iterator() {
            return m.valuesIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static final class KeyBufferSet<K, V> extends AbstractSet<OakUnscopedBuffer> {

        private final OakHashMap<K, V> m;

        KeyBufferSet(OakHashMap<K, V> oakHashMap) {
            this.m = oakHashMap;
        }

        @Override
        public Iterator<OakUnscopedBuffer> iterator() {
            return m.keysBufferIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static class EntryBufferSet<K, V> extends AbstractSet<Entry<OakUnscopedBuffer, OakUnscopedBuffer>> {
        private final OakHashMap<K, V> m;

        EntryBufferSet(OakHashMap<K, V> m) {
            this.m = m;
        }

        @Override
        public Iterator<Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iterator() {
            return m.entriesBufferIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static final class ValueBuffers<K, V> extends AbstractCollection<OakUnscopedBuffer> {

        private final OakHashMap<K, V> m;

        ValueBuffers(OakHashMap<K, V> oakHashMap) {
            this.m = oakHashMap;
        }

        @Override
        public Iterator<OakUnscopedBuffer> iterator() {
            return m.valuesBufferIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static final class KeyStreamBufferSet<K, V> extends AbstractSet<OakUnscopedBuffer> {

        private final OakHashMap<K, V> m;

        KeyStreamBufferSet(OakHashMap<K, V> oakHashMap) {
            this.m = oakHashMap;
        }

        @Override
        public Iterator<OakUnscopedBuffer> iterator() {
            return m.keysStreamIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static class EntryStreamBufferSet<K, V> extends AbstractSet<Entry<OakUnscopedBuffer, OakUnscopedBuffer>> {
        private final OakHashMap<K, V> m;

        EntryStreamBufferSet(OakHashMap<K, V> m) {
            this.m = m;
        }

        @Override
        public Iterator<Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iterator() {
            return m.entriesStreamIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static final class ValueStreamBuffers<K, V> extends AbstractCollection<OakUnscopedBuffer> {

        private final OakHashMap<K, V> m;

        ValueStreamBuffers(OakHashMap<K, V> m) {
            this.m = m;
        }

        @Override
        public Iterator<OakUnscopedBuffer> iterator() {
            return m.valuesStreamIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }
}
