/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class OakMap<K, V> {

  private InternalOakMap internalOakMap;
  final OakMemoryManager memoryManager;
  private Function<ByteBuffer, K> keyDeserializeTransformer;
  private Function<ByteBuffer, V> valueDeserializeTransformer;
  private Function<Map.Entry<ByteBuffer, ByteBuffer>, Map.Entry<K, V>> entryDeserializeTransformer;
  public final Comparator comparator;

  // SubOakMap fields
  private K fromKey;
  private boolean fromInclusive;
  private K toKey;
  private boolean toInclusive;
  private boolean isDescending;

  public OakMap(K minKey,
                Serializer<K> keySerializer,
                Serializer<V> valueSerializer,
                OakComparator<K> oakComparator,
                MemoryPool memoryPool,
                int chunkMaxItems,
                int chunkBytesPerItem) {

    this.comparator = new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
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
      }
    };

    this.memoryManager = new OakMemoryManager(memoryPool);
    this.internalOakMap = new InternalOakMap(minKey, keySerializer, valueSerializer, this.comparator,
            this.memoryManager, chunkMaxItems, chunkBytesPerItem);
    this.fromKey = null;
    this.fromInclusive = false;
    this.toKey = null;
    this.isDescending = false;



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
    this.entryDeserializeTransformer = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Map.Entry<K, V>>() {
      @Override
      public Map.Entry<K, V> apply(Map.Entry<ByteBuffer, ByteBuffer> entry) {
        return new AbstractMap.SimpleEntry<K, V>(keySerializer.deserialize(entry.getKey()), valueSerializer.deserialize(entry.getValue()));
      }
    };
  }

  private OakMap(InternalOakMap internalOakMap, OakMemoryManager memoryManager,
                 Function<ByteBuffer, K> keyDeserializeTransformer,
                 Function<ByteBuffer, V> valueDeserializeTransformer,
                 Function<Map.Entry<ByteBuffer, ByteBuffer>, Map.Entry<K, V>> entryDeserializeTransformer,
                 Comparator comparator,
                 K fromKey, boolean fromInclusive, K toKey,
                 boolean toInclusive, boolean isDescending) {
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

  /*-------------- size --------------*/

  /**
   * @return current off heap memory usage in bytes
   */
  public long memorySize() {
    return internalOakMap.memorySize();
  }

  /**
   * @return the current number of keys in the map.
   * Isn't supported for SubMaps
   */
  public int entries() {
    if (this.fromKey != null || this.toKey != null) {
      // this is a SubMap, for SubMap number of keys can not be counted
      throw new UnsupportedOperationException();
    }

    return internalOakMap.entries();
  }

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
  public void put(K key, V value) {
    if (key == null || value == null)
      throw new NullPointerException();
    if (!inBounds(key))
      throw new IllegalArgumentException();

    memoryManager.startThread();
    internalOakMap.put(key, value);
    memoryManager.stopThread();
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
    if (key == null || value == null)
      throw new NullPointerException();
    if (!inBounds(key))
      throw new IllegalArgumentException();

    memoryManager.startThread();
    boolean result = internalOakMap.putIfAbsent(key, value);
    memoryManager.stopThread();
    return result;
  }

  /**
   * Removes the mapping for a key from this map if it is present.
   *
   * @param key key whose mapping is to be removed from the map
   * @throws NullPointerException if the specified key is null
   */
  void remove(K key) {
    if (key == null)
      throw new NullPointerException();
    if (!inBounds(key))
      throw new IllegalArgumentException();

    memoryManager.startThread();
    internalOakMap.remove(key);
    memoryManager.stopThread();
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
    if (key == null)
      throw new NullPointerException();
    if (!inBounds(key))
      throw new IllegalArgumentException();

    memoryManager.startThread();
    V value = (V) internalOakMap.getValueTransformation(key, valueDeserializeTransformer);
    memoryManager.stopThread();
    return value;
  }

  /**
   * Returns the minimal key in the map,
   * or {@code null} if this map contains no keys.
   *
   * @return the minimal key in the map,
   * or {@code null} if this map contains no keys.
   */
  K getMinKey() {
    if (this.fromKey != null || this.toKey != null) {
      throw new UnsupportedOperationException();
    }

    memoryManager.startThread();
    K minKey = (K) internalOakMap.getMinKeyTransformation(keyDeserializeTransformer);
    memoryManager.stopThread();
    return minKey;
  }

  /**
   * Returns the maximal key in the map,
   * or {@code null} if this map contains no keys.
   *
   * @return the maximal key in the map,
   * or {@code null} if this map contains no keys.
   */
  K getMaxKey() {
    if (this.fromKey != null || this.toKey != null) {
      throw new UnsupportedOperationException();
    }

    memoryManager.startThread();
    K maxKey = (K) internalOakMap.getMaxKeyTransformation(keyDeserializeTransformer);
    memoryManager.stopThread();
    return maxKey;
  }

  /**
   * Updates the value for the specified key
   *
   * @param key      key with which the calculation is to be associated
   * @param computer for computing the new value
   * @return {@code false} if there was no mapping for the key
   * @throws NullPointerException if the specified key or the function is null
   */
  boolean computeIfPresent(K key, Consumer<OakWBuffer> computer) {
    if (key == null || computer == null)
      throw new NullPointerException();
    if (!inBounds(key))
      throw new IllegalArgumentException();

    memoryManager.startThread();
    boolean result = internalOakMap.computeIfPresent(key, computer);
    memoryManager.stopThread();
    return result;
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
  void putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer) {
    if (key == null || value == null || computer == null)
      throw new NullPointerException();
    if (!inBounds(key))
      throw new IllegalArgumentException();

    memoryManager.startThread();
    internalOakMap.putIfAbsentComputeIfPresent(key, value, computer);
    memoryManager.stopThread();
  }

  /*-------------- SubMap --------------*/
  // package visibility to be used by the views
  boolean inBounds(K key) {
    int res;
    if (fromKey != null) {
      res = comparator.compare(key, fromKey);
      if (res < 0 || (res == 0 && !fromInclusive))
        return false;
    }

    if (toKey != null) {
      res = comparator.compare(key, toKey);
      if (res > 0 || (res == 0 && !toInclusive))
        return false;
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
  OakMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {

    if (this.comparator.compare(fromKey, toKey) > 0) {
      throw new IllegalArgumentException();
    }
    return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
            this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, fromKey, fromInclusive, toKey, toInclusive, this.isDescending);
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
    if (this.fromKey != null && this.comparator.compare(this.fromKey, toKey) > 0) {
      throw new IllegalArgumentException();
    }

    return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
            this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, this.fromKey, this.fromInclusive, toKey, inclusive, this.isDescending);
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
    if (this.toKey != null && this.comparator.compare(fromKey, this.toKey) > 0) {
      throw new IllegalArgumentException();
    }

    return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
            this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, fromKey, inclusive, this.toKey, this.toInclusive, this.isDescending);
  }

    /* ---------------- Retrieval methods -------------- */

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
  public OakMap descendingMap() {
    return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
            this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, this.fromKey, this.fromInclusive, this.toKey, this.toInclusive, true);
  }

  /**
   * Returns a {@link CloseableIterator} of the values contained in this map
   * in ascending order of the corresponding keys.
   */
  public CloseableIterator<V> valuesIterator() {
    return internalOakMap.valuesTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, valueDeserializeTransformer);
  }

  /**
   * Returns a {@link CloseableIterator} of the mappings contained in this map in ascending key order.
   */
  public CloseableIterator<Map.Entry<K, V>> entriesIterator() {
    return internalOakMap.entriesTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, entryDeserializeTransformer);
  }

  /**
   * Returns a {@link CloseableIterator} of the keys contained in this map in ascending order.
   */
  public CloseableIterator<K> keysIterator() {
    return internalOakMap.keysTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, keyDeserializeTransformer);
  }

  /* ---------------- View methods -------------- */
  /**
   * Return the OakMap view, where the mappings are presented as OakBuffers without costly deserialization
   */
  public OakBufferView createBufferView(){
    return new OakBufferView(internalOakMap,this, fromKey, toKey);
  }

  /**
   * Return the OakMap view, where the mappings are presented as OakBuffers without costly deserialization
   */
  public <T> OakTransformView createTransformView(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer){
    return new OakTransformView<K, T>(internalOakMap,this, fromKey, toKey, transformer);
  }

  /* ---------------- Package visibility getters for the views methods -------------- */
  boolean getIsDescending(){
    return isDescending;
  }

  boolean getFromInclusive() {
    return fromInclusive;
  }

  boolean getToInclusive() {
    return toInclusive;
  }
}
