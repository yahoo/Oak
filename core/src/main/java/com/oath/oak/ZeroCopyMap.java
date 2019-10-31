/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface ZeroCopyMap<K, V> {
    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @throws NullPointerException     if the specified key is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    void put(K key, V value);

    /**
     * Returns a read only view of the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with that key, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException     if the specified key is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    OakRBuffer get(K key);

    /**
     * Removes the mapping for a key from this map if it is present.
     *
     * @param key key whose mapping is to be removed from the map
     * @throws NullPointerException     if the specified key is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     * @return {@code true} if there was a mapping for the key
     */
    boolean remove(K key);

    /**
     * If the specified key is not already associated
     * with a value, associate it with the given value.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return {@code true} if there was no mapping for the key
     * @throws NullPointerException     if the specified key or value is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    boolean putIfAbsent(K key, V value);

    /**
     * Updates the value for the specified key
     *
     * @param key      key with which the calculation is to be associated
     * @param computer for computing the new value
     * @return {@code false} if there was no mapping for the key
     * @throws NullPointerException     if the specified key or the function is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    boolean computeIfPresent(K key, Consumer<OakWBuffer> computer);


    /**
     * If the specified key is not already associated
     * with a value, associate it with a constructed value.
     * Else, updates the value for the specified key.
     *
     * @param key      key with which the specified value is to be associated
     * @param value    value to be associated with the specified key
     * @param computer for computing the new value when the key is present
     * @return {@code true} if there was no mapping for the key
     * @throws NullPointerException     if any of the parameters is null
     * @throws IllegalArgumentException if the specified key is out of bounds
     */
    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer);


    /**
     * Returns a {@link Set} view of read only ByteBuffers containing the
     * serialized keys stored in this map.
     *
     * @return a set view of the serialized keys contained in this map
     */
    Set<OakRBuffer> keySet();

    /**
     * Returns a {@link Collection} view of read only buffers containing the
     * serialized values stored in this map.
     *
     * @return a collection view of the values contained in this map
     */
    Collection<OakRBuffer> values();

    /**
     * Returns a {@link Set} view of the serialized mappings contained in this
     * map.
     *
     * @return a set view of the serialized mappings contained in this map
     */
    Set<Map.Entry<OakRBuffer, OakRBuffer>> entrySet();

    /**
     * Returns a {@link Set} view of read only OakByteBuffers containing the
     * serialized keys stored in this map. When set is iterated it gives a "stream" view
     * on the elements, meaning only one element can be observed at a time.
     * The set iteration can not be shared between multi threads.
     * <p>
     * The stream iterator is intended to be used in threads that are for iterations only
     * and are not involved in concurrent/parallel reading/updating the mappings
     *
     * @return a set view of the serialized keys contained in this map
     */
    Set<OakRBuffer> keyStreamSet();

    /**
     * Returns a {@link Collection} view of read only OakByteBuffers containing the
     * serialized values stored in this map. When set is iterated it gives a "stream" view
     * on the elements, meaning only one element can be observed at a time.
     * The set iteration can not be shared between multi threads.
     * <p>
     * The stream iterator is intended to be used in threads that are for iterations only
     * and are not involved in concurrent/parallel reading/updating the mappings
     *
     * @return a collection view of the values contained in this map
     */
    Collection<OakRBuffer> valuesStream();

    /**
     * Returns a {@link Set} view of the serialized mappings contained in this
     * map. When set is iterated it gives a "stream" view
     * on the elements, meaning only one element can be observed at a time.
     * The set iteration can not be shared between multi threads.
     * <p>
     * The stream iterator is intended to be used in threads that are for iterations only
     * and are not involved in concurrent/parallel reading/updating the mappings
     *
     * @return a set view of the serialized mappings contained in this map
     */
    Set<Map.Entry<OakRBuffer, OakRBuffer>> entryStreamSet();
}
