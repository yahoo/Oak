/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A concurrent map implementation which supports off-heap memory.
 */
public class OakMap<K, V> extends AbstractMap<K, V> implements AutoCloseable, ConcurrentNavigableMap<K, V> {

    private final InternalOakMap<K, V> internalOakMap;
    /*
     * Memory manager cares for allocation, de-allocation and reuse of the internally pre-allocated
     * memory. Each thread that is going to access a memory that can be released by memory must
     * start with startThread() and end with stopOperation(). Those calls can be nested, but amount of
     * attaches must be equal to detach.
     * Attach-Detach Policy:
     * For any externally used Oak class (OakMap, Iterator, OakBuffer- or OakTransform- View),
     * this specific class is responsible to wrap the internal methods with attach-detach.
     * */
    private final MemoryManager memoryManager;
    private final Function<ByteBuffer, K> keyDeserializeTransformer;
    private final Function<ByteBuffer, V> valueDeserializeTransformer;
    private final Function<Map.Entry<ByteBuffer, ByteBuffer>, Map.Entry<K, V>> entryDeserializeTransformer;
    private final Comparator<Object> comparator;

    // SubOakMap fields
    private final K fromKey;
    private final boolean fromInclusive;
    private final K toKey;
    private boolean toInclusive;
    private final boolean isDescending;
    private final ThreadIndexCalculator threadIndexCalculator;

    // internal constructor, to create OakMap use OakMapBuilder
    OakMap(K minKey, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer, OakComparator<K> oakComparator,
           int chunkMaxItems, int chunkBytesPerItem, MemoryManager mm, ThreadIndexCalculator threadIndexCalculator) {

        this.comparator = (o1, o2) -> {
            if (o1 instanceof ByteBuffer) {
                if (o2 instanceof ByteBuffer) {
                    return oakComparator.compareSerializedKeys((ByteBuffer) o1, (ByteBuffer) o2);
                } else {
                    return oakComparator.compareSerializedKeyAndKey((ByteBuffer) o1, (K) o2);
                }
            } else {
                if (o2 instanceof ByteBuffer) {
                    return (-1) * oakComparator.compareSerializedKeyAndKey((ByteBuffer) o2, (K) o1);
                } else {
                    return oakComparator.compareKeys((K) o1, (K) o2);
                }
            }
        };

        this.threadIndexCalculator = threadIndexCalculator;
        this.memoryManager = mm;
        this.internalOakMap = new InternalOakMap<K, V>(minKey, keySerializer, valueSerializer, this.comparator,
                this.memoryManager, chunkMaxItems, chunkBytesPerItem, threadIndexCalculator);
        this.fromKey = null;
        this.fromInclusive = false;
        this.toKey = null;
        this.isDescending = false;

        this.keyDeserializeTransformer = keySerializer::deserialize;
        this.valueDeserializeTransformer = valueSerializer::deserialize;
        this.entryDeserializeTransformer = entry -> new AbstractMap.SimpleEntry<K, V>(keySerializer.deserialize(entry.getKey()), valueSerializer.deserialize(entry.getValue()));
    }

    // set constructor, mostly used for subMap
    private OakMap(InternalOakMap<K, V> internalOakMap, MemoryManager memoryManager,
                   Function<ByteBuffer, K> keyDeserializeTransformer,
                   Function<ByteBuffer, V> valueDeserializeTransformer,
                   Function<Map.Entry<ByteBuffer, ByteBuffer>, Map.Entry<K, V>> entryDeserializeTransformer,
                   Comparator<Object> comparator,
                   K fromKey, boolean fromInclusive, K toKey,
                   boolean toInclusive, boolean isDescending, ThreadIndexCalculator threadIndexCalculator) {
        this.threadIndexCalculator = threadIndexCalculator;
        this.internalOakMap = internalOakMap;
        this.memoryManager = memoryManager;
        this.keyDeserializeTransformer = keyDeserializeTransformer;
        this.valueDeserializeTransformer = valueDeserializeTransformer;
        this.entryDeserializeTransformer = entryDeserializeTransformer;
        this.comparator = comparator;
        this.fromKey = fromKey;
        this.fromInclusive = fromInclusive;
        this.toKey = toKey;
        this.toInclusive = toInclusive;
        this.isDescending = isDescending;
    }

    /* ------ Map API methods ------ */

    /**
     * Returns the current number of key-value mappings in this map.
     * Not supported for SubMaps.
     *
     * @return the number of key-value mappings in this map
     * @throws UnsupportedOperationException if used on a SubMap
     */
    @Override
    public int size() {
        if (this.isSubmap()) throw new UnsupportedOperationException();

        return internalOakMap.entries();
    }

    /**
     * Returns a deserialized copy of the value to which the specified key is
     * mapped, or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with that key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException     if the specified key is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    @Override
    public V get(Object key) {
        checkKey(key);

        return internalOakMap.getValueTransformation((K) key, valueDeserializeTransformer);
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
        if (value == null)
            throw new NullPointerException();

        return (V) internalOakMap.put(key, value, valueDeserializeTransformer);
    }

    /**
     * Removes the mapping for a key from this map if it is present.
     *
     * @param key key whose mapping is to be removed from the map
     * @return the previous value associated with the provided key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException     if the specified key is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    @Override
    public V remove(Object key) {
        checkKey(key);

        return internalOakMap.remove((K) key, null, valueDeserializeTransformer);
    }

    /* ------ SortedMap API methods ------ */

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    /**
     * Returns the minimal key in the map,
     * or {@code null} if this map contains no keys.
     *
     * @return the minimal key in the map, or {@code null} if this map contains
     * no keys.
     * @throws UnsupportedOperationException if used on a SubMap
     */
    @Override
    public K firstKey() {
        // this interface shouldn't be used with subMap
        if (this.isSubmap()) throw new UnsupportedOperationException();

        return (K) internalOakMap.getMinKeyTransformation(keyDeserializeTransformer);
    }

    /**
     * Returns the maximal key in the map,
     * or {@code null} if this map contains no keys.
     *
     * @return the maximal key in the map, or {@code null} if this map contains
     * no keys.
     * @throws UnsupportedOperationException if used on a SubMap
     */
    @Override
    public K lastKey() {
        // this interface shouldn't be used with subMap
        if (this.isSubmap()) throw new UnsupportedOperationException();

        return (K) internalOakMap.getMaxKeyTransformation(keyDeserializeTransformer);
    }

    /* ------ ConcurrentMap API methods ------ */

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object key, Object value) {
        checkKey(key);

        return (value != null) && (internalOakMap.remove((K) key, (V) value, valueDeserializeTransformer) != null);
    }


    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException     if the specified key or value is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    @Override
    public V replace(K key, V value) {
        checkKey(key);
        if (value == null)
            throw new NullPointerException();

        return (V) internalOakMap.replace(key, value, valueDeserializeTransformer);
    }


    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException     if any of the arguments are null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkKey(key);
        if (oldValue == null || newValue == null)
            throw new NullPointerException();

        return internalOakMap.replace(key, oldValue, newValue, valueDeserializeTransformer);
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
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    public V putIfAbsent(K key, V value) {
        checkKey(key);
        if (value == null)
            throw new NullPointerException();

        return (V) internalOakMap.putIfAbsent(key, value, valueDeserializeTransformer).value;
    }


    /* ---------------- NavigableMap API methods -------------- */

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException if used on a SubMap
     */
    @Override
    public Entry<K, V> lowerEntry(K key) {
        if (this.isSubmap()) throw new UnsupportedOperationException();

        if (key == null) throw new NullPointerException();

        return internalOakMap.lowerEntry(key);
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException if used on a SubMap
     */
    @Override
    public K lowerKey(K key) {
        if (this.isSubmap()) throw new UnsupportedOperationException();

        if (key == null) throw new NullPointerException();

        return (K) internalOakMap.lowerEntry(key).getKey();
    }


    /* ---------------- ConcurrentNavigableMap API methods -------------- */

    /*-------------- SubMap --------------*/

    boolean inBounds(Object key) {
        int res;
        if (fromKey != null) {
            res = comparator.compare(key, fromKey);
            if (res < 0 || (res == 0 && !fromInclusive))
                return false;
        }

        if (toKey != null) {
            res = comparator.compare(key, toKey);
            return res <= 0 && (res != 0 || toInclusive);
        }
        return true;
    }

    /**
     * Returns a view of the portion of this map whose keys range from
     * {@code fromKey} to {@code toKey}.  If {@code fromKey} and
     * {@code toKey} are equal, the returned map is empty unless
     * {@code fromInclusive} and {@code toInclusive} are both true.  The
     * returned map is backed by this map, so changes in the returned map are
     * reflected in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>The returned map will throw an {@code IllegalArgumentException}
     * on an attempt to insert a key outside of its range, or to construct a
     * submap either of whose endpoints lie outside its range.
     *
     * @param fromKey       low endpoint of the keys in the returned map
     * @param fromInclusive {@code true} if the low endpoint
     *                      is to be included in the returned view
     * @param toKey         high endpoint of the keys in the returned map
     * @param toInclusive   {@code true} if the high endpoint
     *                      is to be included in the returned view
     * @return a view of the portion of this map whose keys range from
     * {@code fromKey} to {@code toKey}
     * @throws NullPointerException     if {@code fromKey} or {@code toKey}
     *                                  is null and this map does not permit null keys
     * @throws IllegalArgumentException if {@code fromKey} is greater than
     *                                  {@code toKey}; or if this map itself has a restricted
     *                                  range, and {@code fromKey} or {@code toKey} lies
     *                                  outside the bounds of the range
     */
    public OakMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {

        return subMap(fromKey, fromInclusive, toKey, toInclusive, this.isDescending);
    }

    @Override
    public OakMap<K, V> subMap(K fromKey, K toKey) {
        return subMap(fromKey, true, toKey, false, this.isDescending);
    }

    public OakMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, boolean descending) {

        if (this.comparator.compare(fromKey, toKey) > 0) {
            throw new IllegalArgumentException();
        }
        internalOakMap.open();
        return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
                this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, fromKey,
                fromInclusive, toKey, toInclusive, descending, threadIndexCalculator);
    }

    /**
     * Returns a view of the portion of this map whose keys are less than (or
     * equal to, if {@code inclusive} is true) {@code toKey}.  The returned
     * map is backed by this map, so changes in the returned map are reflected
     * in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>The returned map will throw an {@code IllegalArgumentException}
     * on an attempt to insert a key outside its range.
     *
     * @param toKey     high endpoint of the keys in the returned map
     * @param inclusive {@code true} if the high endpoint
     *                  is to be included in the returned view
     * @return a view of the portion of this map whose keys are less than
     * (or equal to, if {@code inclusive} is true) {@code toKey}
     * @throws NullPointerException     if {@code toKey} is null
     *                                  and this map does not permit null keys
     * @throws IllegalArgumentException if this map itself has a
     *                                  restricted range, and {@code toKey} lies outside the
     *                                  bounds of the range
     */
    public OakMap<K, V> headMap(K toKey, boolean inclusive) {
        if (this.fromKey != null && this.comparator.compare(this.fromKey, toKey) > 0) {
            throw new IllegalArgumentException();
        }
        internalOakMap.open();
        return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
                this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, this.fromKey,
                this.fromInclusive, toKey, inclusive, this.isDescending, threadIndexCalculator);
    }


    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        return headMap(toKey, false);
    }

    /**
     * Returns a view of the portion of this map whose keys are greater than (or
     * equal to, if {@code inclusive} is true) {@code fromKey}.  The returned
     * map is backed by this map, so changes in the returned map are reflected
     * in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>The returned map will throw an {@code IllegalArgumentException}
     * on an attempt to insert a key outside its range.
     *
     * @param fromKey   low endpoint of the keys in the returned map
     * @param inclusive {@code true} if the low endpoint
     *                  is to be included in the returned view
     * @return a view of the portion of this map whose keys are greater than
     * (or equal to, if {@code inclusive} is true) {@code fromKey}
     * @throws NullPointerException     if {@code fromKey} is null
     *                                  and this map does not permit null keys
     * @throws IllegalArgumentException if this map itself has a
     *                                  restricted range, and {@code fromKey} lies outside the
     *                                  bounds of the range
     */
    public OakMap<K, V> tailMap(K fromKey, boolean inclusive) {
        if (this.toKey != null && this.comparator.compare(fromKey, this.toKey) > 0) {
            throw new IllegalArgumentException();
        }
        internalOakMap.open();
        return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
                this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, fromKey,
                inclusive, this.toKey, this.toInclusive, this.isDescending, threadIndexCalculator);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return tailMap(fromKey, true);
    }

    /**
     * Returns a reverse order view of the mappings contained in this map.
     * The descending map is backed by this map, so changes to the map are
     * reflected in the descending map, and vice-versa.
     * <p>The expression {@code m.descendingMap().descendingMap()} returns a
     * view of {@code m} essentially equivalent to {@code m}.
     *
     * @return a reverse order view of this map
     */
    public OakMap<K, V> descendingMap() {
        internalOakMap.open();
        return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
                this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator,
                this.fromKey, this.fromInclusive, this.toKey, this.toInclusive, true, threadIndexCalculator);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return new KeySet<>(this);
    }

    @Override
    public NavigableSet<K> keySet() {
        return new KeySet<>(this);
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return descendingMap().navigableKeySet();
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

    public static class OakZeroCopyMap<K, V> implements ZeroCopyMap<K, V> {
        private OakMap<K, V> m;

        OakZeroCopyMap(OakMap<K, V> kvOakMap) {
            this.m = kvOakMap;
        }

        public void put(K key, V value) {
            m.checkKey(key);
            if (value == null)
                throw new NullPointerException();

            m.internalOakMap.put(key, value, null);
        }

        public OakRBuffer get(K key) {
            m.checkKey(key);

            return m.internalOakMap.get(key);
        }

        public void remove(Object key) {
            m.checkKey(key);

            m.internalOakMap.remove((K) key, null, null);
        }

        public boolean putIfAbsent(K key, V value) {
            m.checkKey(key);
            if (value == null)
                throw new NullPointerException();

            return m.internalOakMap.putIfAbsent(key, value, null).flag;
        }

        public boolean computeIfPresent(K key, Consumer<OakWBuffer> computer) {
            m.checkKey(key);
            if (computer == null)
                throw new NullPointerException();

            return m.internalOakMap.computeIfPresent(key, computer);
        }


        public boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer) {
            m.checkKey(key);
            if (value == null || computer == null)
                throw new IllegalArgumentException();

            return m.internalOakMap.putIfAbsentComputeIfPresent(key, value, computer);
        }


        public Set<OakRBuffer> keySet() {
            return new KeyBufferSet<>(m);
        }

        public Collection<OakRBuffer> values() {
            return new ValueBuffers<>(m);
        }

        public Set<Entry<OakRBuffer, OakRBuffer>> entrySet() {
            return new EntryBufferSet<>(m);
        }
    }


    /* ----------- Oak misc methods ----------- */

    /**
     * @return current off heap memory usage in bytes
     */
    public long memorySize() {
        return internalOakMap.memorySize();
    }


    /* ---------------- Package visibility getters for the views methods -------------- */
    MemoryManager getMemoryManager() {
        return memoryManager;
    }

    boolean getIsDescending() {
        return isDescending;
    }

    boolean getFromInclusive() {
        return fromInclusive;
    }

    boolean getToInclusive() {
        return toInclusive;
    }

    @Override
    public void close() {
        internalOakMap.close();
    }


    /* ---------------- Private utility methods -------------- */

    private void checkKey(Object key) {
        if (key == null)
            throw new NullPointerException();
        if (!inBounds(key))
            throw new IllegalArgumentException("The key is out of map bounds");

    }

    private boolean isSubmap() {
        return (this.fromKey != null || this.toKey != null);
    }

    /**
     * Returns a {@link Iterator} of the values contained in this map
     * in ascending order of the corresponding keys.
     */
    private Iterator<V> valuesIterator() {
        return internalOakMap.valuesTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, valueDeserializeTransformer);
    }

    /**
     * Returns a {@link Iterator} of the mappings contained in this map in ascending key order.
     */
    private Iterator<Map.Entry<K, V>> entriesIterator() {
        return internalOakMap.entriesTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, entryDeserializeTransformer);
    }

    /**
     * Returns a {@link Iterator} of the keys contained in this map in ascending order.
     */
    private Iterator<K> keysIterator() {
        return internalOakMap.keysTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, keyDeserializeTransformer);
    }

    private Iterator<OakRBuffer> keysBufferIterator() {
        return internalOakMap.keysBufferViewIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending);
    }


    private Iterator<OakRBuffer> valuesBufferIterator() {
        return internalOakMap.valuesBufferViewIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending);
    }

    private Iterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesBufferIterator() {
        return internalOakMap.entriesBufferViewIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending);
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

    @Override
    public Entry<K, V> floorEntry(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K floorKey(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K ceilingKey(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K higherKey(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> firstEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> lastEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        throw new UnsupportedOperationException();
    }

    /* ---------------- View Classes -------------- */

    static class KeySet<K> extends AbstractSet<K> implements NavigableSet<K> {

        private final OakMap<K, ?> m;

        KeySet(OakMap<K, ?> m) {
            this.m = m;
        }

        @Override
        public K lower(K k) {
            return m.lowerKey(k);
        }

        @Override
        public K floor(K k) {
            return m.floorKey(k);
        }

        @Override
        public K ceiling(K k) {
            return m.ceilingKey(k);
        }

        @Override
        public K higher(K k) {
            return m.higherKey(k);
        }

        @Override
        public K pollFirst() {
            Map.Entry<K, ?> e = m.pollFirstEntry();
            return (e == null) ? null : e.getKey();
        }

        @Override
        public K pollLast() {
            Map.Entry<K, ?> e = m.pollLastEntry();
            return (e == null) ? null : e.getKey();
        }

        @Override
        public Iterator<K> iterator() {
            return m.keysIterator();
        }

        @Override
        public NavigableSet<K> descendingSet() {
            return new KeySet<>(m.descendingMap());
        }

        @Override
        public Iterator<K> descendingIterator() {
            return descendingSet().iterator();
        }

        @Override
        public NavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive) {
            return new KeySet<>(m.subMap(fromElement, fromInclusive, toElement, toInclusive));
        }

        @Override
        public NavigableSet<K> headSet(K toElement, boolean inclusive) {
            return new KeySet<>(m.headMap(toElement, inclusive));
        }

        @Override
        public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
            return new KeySet<>(m.tailMap(fromElement, inclusive));
        }

        @Override
        public Comparator<? super K> comparator() {
            return m.comparator();
        }

        @Override
        public SortedSet<K> subSet(K fromElement, K toElement) {
            return subSet(fromElement, true, toElement, false);
        }

        @Override
        public SortedSet<K> headSet(K toElement) {
            return headSet(toElement, false);
        }

        @Override
        public SortedSet<K> tailSet(K fromElement) {
            return tailSet(fromElement, true);
        }

        @Override
        public K first() {
            return m.firstKey();
        }

        @Override
        public K last() {
            return m.lastKey();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
        private final OakMap<K, V> m;

        EntrySet(OakMap<K, V> m) {
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

        private final OakMap<?, V> m;

        public Values(OakMap<?, V> oakMap) {
            this.m = oakMap;
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

    static final class KeyBufferSet<K, V> extends AbstractSet<OakRBuffer> {

        private final OakMap<K, V> m;

        public KeyBufferSet(OakMap<K, V> oakMap) {
            this.m = oakMap;
        }

        @Override
        public Iterator<OakRBuffer> iterator() {
            return m.keysBufferIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static class EntryBufferSet<K, V> extends AbstractSet<Entry<OakRBuffer, OakRBuffer>> {
        private final OakMap<K, V> m;

        EntryBufferSet(OakMap<K, V> m) {
            this.m = m;
        }

        @Override
        public Iterator<Entry<OakRBuffer, OakRBuffer>> iterator() {
            return m.entriesBufferIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

    static final class ValueBuffers<K, V> extends AbstractCollection<OakRBuffer> {

        private final OakMap<K, V> m;

        public ValueBuffers(OakMap<K, V> oakMap) {
            this.m = oakMap;
        }

        @Override
        public Iterator<OakRBuffer> iterator() {
            return m.valuesBufferIterator();
        }

        @Override
        public int size() {
            return m.size();
        }
    }

}
