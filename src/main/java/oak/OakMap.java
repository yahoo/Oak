/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

public interface OakMap<K, V> {

    /* ------ Map API methods ------ */

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @throws NullPointerException if the specified key is null
     */
    void put(K key, V value);

    /**
     * If the specified key is not already associated
     * with a value, associate it with the given value.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return {@code true} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    boolean putIfAbsent(K key, V value);

    /**
     * Removes the mapping for a key from this map if it is present.
     *
     * @param key key whose mapping is to be removed from the map
     * @throws NullPointerException if the specified key is null
     */
    void remove(K key);

    /**
     * Returns a read only view of the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with that key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException if the specified key is null
     */
    V get(K key);

    /**
     * Returns a transformation of the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @param transformer the function that executes the transformation
     * @return a transformation of the value to which the specified key is mapped, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException if the specified key is null
     */
    <T> T getTransformation(K key, Function<ByteBuffer,T> transformer);

    /**
     * Returns the minimal key in the map,
     * or {@code null} if this map contains no keys.
     *
     * @return the minimal key in the map,
     * or {@code null} if this map contains no keys.
     */
     K getMinKey();

    /**
     * Returns the maximal key in the map,
     * or {@code null} if this map contains no keys.
     *
     * @return the maximal key in the map,
     * or {@code null} if this map contains no keys.
     */
     K getMaxKey();

    /**
     * Updates the value for the specified key
     *
     * @param key      key with which the calculation is to be associated
     * @param computer for computing the new value
     * @return {@code false} if there was no mapping for the key
     * @throws NullPointerException if the specified key or the function is null
     */
    boolean computeIfPresent(K key, Consumer<ByteBuffer> computer);

    /**
     * If the specified key is not already associated
     * with a value, associate it with a constructed value.
     * Else, updates the value for the specified key.
     *
     * @param key         key with which the specified value is to be associated
     * @param value       value to be associated with the specified key
     * @param computer    for computing the new value when the key is present
     */
    void putIfAbsentComputeIfPresent(K key, V value, Consumer<ByteBuffer> computer);

    /**
     * returns the size in bytes of the occupied memory
     */
    long memorySize();

    /*-------------- SubMap --------------*/

    /**
     * Returns a view of the portion of this map whose keys range from
     * {@code fromKey} to {@code toKey}.  If {@code fromKey} and
     * {@code toKey} are equal, the returned map is empty unless
     * {@code fromInclusive} and {@code toInclusive} are both true.  The
     * returned map is backed by this map, so changes in the returned map are
     * reflected in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>
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
    OakMap subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

    /**
     * Returns a view of the portion of this map whose keys are less than (or
     * equal to, if {@code inclusive} is true) {@code toKey}.  The returned
     * map is backed by this map, so changes in the returned map are reflected
     * in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>
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
    OakMap headMap(K toKey, boolean inclusive);

    /**
     * Returns a view of the portion of this map whose keys are greater than (or
     * equal to, if {@code inclusive} is true) {@code fromKey}.  The returned
     * map is backed by this map, so changes in the returned map are reflected
     * in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>
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
    OakMap tailMap(K fromKey, boolean inclusive);

    /* ---------------- View methods -------------- */

    /**
     * Returns a reverse order view of the mappings contained in this map.
     * The descending map is backed by this map, so changes to the map are
     * reflected in the descending map, and vice-versa.
     * <p>
     * <p>The expression {@code m.descendingMap().descendingMap()} returns a
     * view of {@code m} essentially equivalent to {@code m}.
     *
     * @return a reverse order view of this map
     */
    OakMap descendingMap();

    /**
     * Returns a {@link CloseableIterator} of the values contained in this map
     * in ascending order of the corresponding keys.
     */
    CloseableIterator<V> valuesIterator();

    /**
     * Returns a {@link CloseableIterator} of the mappings contained in this map in ascending key order.
     */
    CloseableIterator<Entry<K, V>> entriesIterator();

    /**
     * Returns a {@link CloseableIterator} of the keys contained in this map in ascending order.
     */
    CloseableIterator<K> keysIterator();

    /**
     * Returns a {@link CloseableIterator} of transformations on the values contained in this map
     * in ascending order of the corresponding keys.
     */
    <T> CloseableIterator<T> valuesTransformIterator(Function<ByteBuffer,T> transformer);

    /**
     * Returns a {@link CloseableIterator} of transformations on the mappings contained in this map
     * in ascending key order.
     */
    <T> CloseableIterator<T> entriesTransformIterator(Function<Entry<ByteBuffer, ByteBuffer>,T> transformer);

    /**
     * Returns a {@link CloseableIterator} of transformations on the keys contained in this map in ascending order.
     */
    <T> CloseableIterator<T> keysTransformIterator(Function<ByteBuffer,T> transformer);
}