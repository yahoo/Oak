/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A concurrent map implementation which supports off-heap memory.
 */
public class OakMap<K, V> implements AutoCloseable {

  private InternalOakMap internalOakMap;
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
  private Function<ByteBuffer, K> keyDeserializeTransformer;
  private Function<ByteBuffer, V> valueDeserializeTransformer;
  private Function<Map.Entry<ByteBuffer, ByteBuffer>, Map.Entry<K, V>> entryDeserializeTransformer;
  final Comparator comparator;

  // SubOakMap fields
  private K fromKey;
  private boolean fromInclusive;
  private K toKey;
  private boolean toInclusive;
  private boolean isDescending;

  // internal constructor, to create OakMap use OakMapBuilder
  OakMap(K minKey, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer, OakComparator<K> oakComparator,
      int chunkMaxItems, int chunkBytesPerItem, MemoryManager mm) {

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

    this.memoryManager = mm;
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

  // set constructor, mostly used for subMap
  private OakMap(InternalOakMap internalOakMap, MemoryManager memoryManager,
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
   * @throws IllegalArgumentException if the specified key is null
   */
  public void put(K key, V value) {
    if (key == null || value == null)
      throw new IllegalArgumentException();
    if (!inBounds(key))
      throw new IllegalArgumentException("The key is out of map bounds");
    try {
      memoryManager.startOperation();
      internalOakMap.put(key, value);
    } finally {
      memoryManager.stopOperation();
    }
  }

  /**
   * If the specified key is not already associated
   * with a value, associate it with the given value.
   * Creates a copy of the value in the map.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return {@code true} if there was no mapping for the key
   * @throws IllegalArgumentException if the specified key or value is null
   */
  public boolean putIfAbsent(K key, V value) {
    if (key == null || value == null)
      throw new IllegalArgumentException();
    if (!inBounds(key))
      throw new IllegalArgumentException("The key is out of map bounds");
    try {
      memoryManager.startOperation();
      return internalOakMap.putIfAbsent(key, value);
    } finally {
      memoryManager.stopOperation();
    }
  }

  /**
   * Removes the mapping for a key from this map if it is present.
   *
   * @param key key whose mapping is to be removed from the map
   * @throws IllegalArgumentException if the specified key is null
   */
  public void remove(K key) {
    if (key == null)
      throw new IllegalArgumentException();
    if (!inBounds(key))
      throw new IllegalArgumentException("The key is out of map bounds");
    try {
      memoryManager.startOperation();
      internalOakMap.remove(key);
    } finally {
      memoryManager.stopOperation();
    }
  }

  /**
   * Returns a read only view of the value to which the specified key is mapped,
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @return the value associated with that key, or
   * {@code null} if this map contains no mapping for the key
   * @throws IllegalArgumentException if the specified key is null
   */
  public V get(K key) {
    if (key == null)
      throw new IllegalArgumentException();
    if (!inBounds(key))
      throw new IllegalArgumentException("The key is out of map bounds");
    try {
      memoryManager.startOperation();
      return (V) internalOakMap.getValueTransformation(key, valueDeserializeTransformer);
    } finally {
      memoryManager.stopOperation();
    }
  }

  /**
   * Returns the minimal key in the map,
   * or {@code null} if this map contains no keys.
   *
   * @return the minimal key in the map,
   * or {@code null} if this map contains no keys.
   */
  public K getMinKey() {
    if (this.fromKey != null || this.toKey != null) {
      // this interface shouldn't be used with subMap
      throw new UnsupportedOperationException();
    }
    try {
      memoryManager.startOperation();
      return (K) internalOakMap.getMinKeyTransformation(keyDeserializeTransformer);
    } finally {
      memoryManager.stopOperation();
    }
  }

  /**
   * Returns the maximal key in the map,
   * or {@code null} if this map contains no keys.
   *
   * @return the maximal key in the map,
   * or {@code null} if this map contains no keys.
   */
  public K getMaxKey() {
    if (this.fromKey != null || this.toKey != null) {
      // this interface shouldn't be used with subMap
      throw new UnsupportedOperationException();
    }
    try {
      memoryManager.startOperation();
      return (K) internalOakMap.getMaxKeyTransformation(keyDeserializeTransformer);
    } finally {
      memoryManager.stopOperation();
    }
  }

  /**
   * Updates the value for the specified key
   *
   * @param key      key with which the calculation is to be associated
   * @param computer for computing the new value
   * @return {@code false} if there was no mapping for the key
   * @throws IllegalArgumentException if the specified key or the function is null
   */
  public boolean computeIfPresent(K key, Consumer<OakWBuffer> computer) {
    if (key == null || computer == null)
      throw new IllegalArgumentException();
    if (!inBounds(key))
      throw new IllegalArgumentException("The key is out of map bounds");
    try {
      memoryManager.startOperation();
      return internalOakMap.computeIfPresent(key, computer);
    } finally {
      memoryManager.stopOperation();
    }
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
  public void putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer) {
    if (key == null || value == null || computer == null)
      throw new IllegalArgumentException();
    if (!inBounds(key))
      throw new IllegalArgumentException("The key is out of map bounds");
    try {
      memoryManager.startOperation();
      internalOakMap.putIfAbsentComputeIfPresent(key, value, computer);
    } finally {
      memoryManager.stopOperation();
    }
  }

  /*-------------- SubMap --------------*/
  // package visibility to be used by the views
  public boolean inBounds(K key) {
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
  public OakMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {

    if (this.comparator.compare(fromKey, toKey) > 0) {
      throw new IllegalArgumentException();
    }
    internalOakMap.open();
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
  public OakMap<K,V> headMap(K toKey, boolean inclusive) {
    if (this.fromKey != null && this.comparator.compare(this.fromKey, toKey) > 0) {
      throw new IllegalArgumentException();
    }
    internalOakMap.open();
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
  public OakMap<K, V> tailMap(K fromKey, boolean inclusive) {
    if (this.toKey != null && this.comparator.compare(fromKey, this.toKey) > 0) {
      throw new IllegalArgumentException();
    }
    internalOakMap.open();
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
  public OakMap<K, V> descendingMap() {
    internalOakMap.open();
    return new OakMap<K, V>(this.internalOakMap, this.memoryManager, this.keyDeserializeTransformer,
            this.valueDeserializeTransformer, this.entryDeserializeTransformer, this.comparator, this.fromKey, this.fromInclusive, this.toKey, this.toInclusive, true);
  }

  /**
   * Returns a {@link OakCloseableIterator} of the values contained in this map
   * in ascending order of the corresponding keys.
   */
  public OakCloseableIterator<V> valuesIterator() {
    return internalOakMap.valuesTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, valueDeserializeTransformer);
  }

  /**
   * Returns a {@link OakCloseableIterator} of the mappings contained in this map in ascending key order.
   */
  public OakCloseableIterator<Map.Entry<K, V>> entriesIterator() {
    return internalOakMap.entriesTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, entryDeserializeTransformer);
  }

  /**
   * Returns a {@link OakCloseableIterator} of the keys contained in this map in ascending order.
   */
  public OakCloseableIterator<K> keysIterator() {
    return internalOakMap.keysTransformIterator(fromKey, fromInclusive, toKey, toInclusive, isDescending, keyDeserializeTransformer);
  }

  /* ---------------- View methods -------------- */
  /**
   * Return the OakMap view, where the mappings are presented as OakBuffers without costly deserialization
   */
  public OakBufferView<K> createBufferView(){
    return new OakBufferView(internalOakMap,this, fromKey, toKey);
  }

  /**
   * Return the OakMap view, where the mappings are presented as OakBuffers without costly deserialization
   */
  public <T> OakTransformView<K, T> createTransformView(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer){
    return new OakTransformView<K, T>(internalOakMap,this, fromKey, toKey, transformer);
  }

  /* ---------------- Package visibility getters for the views methods -------------- */
  MemoryManager getMemoryManager() { return memoryManager; }

  boolean getIsDescending(){
    return isDescending;
  }

  boolean getFromInclusive() {
    return fromInclusive;
  }

  boolean getToInclusive() {
    return toInclusive;
  }

  // For internal tests only
  void assertIfNotIdle() {
    memoryManager.assertIfNotIdle();
  }

  /**
   * Closes this resource, relinquishing any underlying resources.
   * This method is invoked automatically on objects managed by the
   * {@code try}-with-resources statement.
   * <p>While this interface method is declared to throw {@code
   * Exception}, implementers are <em>strongly</em> encouraged to
   * declare concrete implementations of the {@code close} method to
   * throw more specific exceptions, or to throw no exception at all
   * if the close operation cannot fail.
   * <p> Cases where the close operation may fail require careful
   * attention by implementers. It is strongly advised to relinquish
   * the underlying resources and to internally <em>mark</em> the
   * resource as closed, prior to throwing the exception. The {@code
   * close} method is unlikely to be invoked more than once and so
   * this ensures that the resources are released in a timely manner.
   * Furthermore it reduces problems that could arise when the resource
   * wraps, or is wrapped, by another resource.
   * <p><em>Implementers of this interface are also strongly advised
   * to not have the {@code close} method throw {@link
   * InterruptedException}.</em>
   * This exception interacts with a thread's interrupted status,
   * and runtime misbehavior is likely to occur if an {@code
   * InterruptedException} is {@linkplain Throwable#addSuppressed
   * suppressed}.
   * More generally, if it would cause problems for an
   * exception to be suppressed, the {@code AutoCloseable.close}
   * method should not throw it.
   * <p>Note that unlike the {@link Closeable#close close}
   * method of {@link Closeable}, this {@code close} method
   * is <em>not</em> required to be idempotent.  In other words,
   * calling this {@code close} method more than once may have some
   * visible side effect, unlike {@code Closeable.close} which is
   * required to have no effect if called more than once.
   * However, implementers of this interface are strongly encouraged
   * to make their {@code close} methods idempotent.
   *
   * @throws Exception if this resource cannot be closed
   */
  @Override public void close() {
    internalOakMap.close();
  }
}
