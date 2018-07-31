/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class OakMap<K, V> implements AutoCloseable {

  private InternalOakMap internalOakMap;
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;
  private Function<ByteBuffer, K> keyDeserializeTransformer;
  private Function<ByteBuffer, V> valueDeserializeTransformer;

  public OakMap(K minKey,
                Serializer<K> keySerializer,
                Serializer<V> valueSerializer,
                OakComparator<K> comparator,
                MemoryPool memoryPool,
                int chunkMaxItems,
                int chunkBytesPerItem) {

    this.internalOakMap = new InternalOakMap(minKey,
            keySerializer,
            valueSerializer,
            comparator,
            memoryPool,
            chunkMaxItems,
            chunkBytesPerItem);
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyDeserializeTransformer = new Function<ByteBuffer, K>() {
      @Override
      public K apply(ByteBuffer byteBuffer) {
        return keySerializer.deserialize(byteBuffer);
      }
    };
    this.valueDeserializeTransformer = new Function<ByteBuffer, V>() {
      @Override
      public V apply(ByteBuffer byteBuffer) {
        return valueSerializer.deserialize(byteBuffer);
      }
    };
  }

  /*-------------- Closable --------------*/

  /**
   * cleans off heap memory
   */
  @Override
  public void close() {
    internalOakMap.close();
  }

  /*-------------- size --------------*/

  /**
   * @return current off heap memory usage in bytes
   */
  public long memorySize() {
    return internalOakMap.memorySize();
  }

  public int entries() { return internalOakMap.entries(); }

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
  void put(K key, V value) {
    internalOakMap.put(key, value);
  }

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
  boolean putIfAbsent(K key, V value) {
    return internalOakMap.putIfAbsent(key, value);
  }

  /**
   * Removes the mapping for a key from this map if it is present.
   *
   * @param key key whose mapping is to be removed from the map
   * @throws NullPointerException if the specified key is null
   */
  void remove(K key) {
    internalOakMap.remove(key);
  }

  /**
   * Returns a read only view of the value to which the specified key is mapped,
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @return the value associated with that key, or
   * {@code null} if this map contains no mapping for the key
   * @throws NullPointerException if the specified key is null
   */
  V get(K key) {
    return (V) internalOakMap.getValueTransformation(key, valueDeserializeTransformer);
  }

  /**
   * Returns the minimal key in the map,
   * or {@code null} if this map contains no keys.
   *
   * @return the minimal key in the map,
   * or {@code null} if this map contains no keys.
   */
  K getMinKey() {
    return (K) internalOakMap.getMinKeyTransformation(keyDeserializeTransformer);
  }

  /**
   * Returns the maximal key in the map,
   * or {@code null} if this map contains no keys.
   *
   * @return the maximal key in the map,
   * or {@code null} if this map contains no keys.
   */
  K getMaxKey() {
    return (K) internalOakMap.getMaxKeyTransformation(keyDeserializeTransformer);
  }

  /**
   * Updates the value for the specified key
   *
   * @param key      key with which the calculation is to be associated
   * @param computer for computing the new value
   * @return {@code false} if there was no mapping for the key
   * @throws NullPointerException if the specified key or the function is null
   */
  boolean computeIfPresent(K key, Consumer<ByteBuffer> computer) {
    return internalOakMap.computeIfPresent(key, computer);
  }

  /**
   * If the specified key is not already associated
   * with a value, associate it with a constructed value.
   * Else, updates the value for the specified key.
   *
   * @param key         key with which the specified value is to be associated
   * @param value       value to be associated with the specified key
   * @param computer    for computing the new value when the key is present
   */
  void putIfAbsentComputeIfPresent(K key, V value, Consumer<ByteBuffer> computer) {
    internalOakMap.putIfAbsentComputeIfPresent(key, value, computer);
  }

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
  OakMap subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    //TODO
    return null;
  }

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
  OakMap headMap(K toKey, boolean inclusive) {
    //TODO
    return null;
  }

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
  OakMap tailMap(K fromKey, boolean inclusive) {
    //TODO
    return null;
  }

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
  OakMap descendingMap() {
    //TODO
    return null;
  }

  /**
   * Returns a {@link CloseableIterator} of the values contained in this map
   * in ascending order of the corresponding keys.
   */
  CloseableIterator<V> valuesIterator() {
    //TODO
    return null;
  }

  /**
   * Returns a {@link CloseableIterator} of the mappings contained in this map in ascending key order.
   */
  CloseableIterator<Map.Entry<K, V>> entriesIterator() {
    //TODO
    return null;
  }

  /**
   * Returns a {@link CloseableIterator} of the keys contained in this map in ascending order.
   */
  CloseableIterator<K> keysIterator() {
    //TODO
    return null;
  }

}
