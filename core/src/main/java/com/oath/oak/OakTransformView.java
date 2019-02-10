/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.function.Function;
import java.util.Map;

/**
 * This class allows data retrieval from OakMap while the output is set as transformed mappings,
 * using a user-supplied transform function
 *
 * @param <K> The key object type.
 * @param <T> The transformation object type
 */

public class OakTransformView<K, T> implements AutoCloseable{

  private final InternalOakMap internalOakMap;  // to hold the actual data
  private final OakMap externalOakMap;          // to hold the boundaries and memory manager
  private final K fromKey;
  private final K toKey;
  private final Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;
  private final Function<ByteBuffer, T> valuesTransformer;
  private final Function<ByteBuffer, T> keysTransformer;

  // package level view constructor, to be executed by OakMap only
  OakTransformView(InternalOakMap iOM, OakMap eOM, K fromKey, K toKey,
                   Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
    this.internalOakMap = iOM;
    internalOakMap.open();
    this.externalOakMap = eOM;
    this.fromKey = fromKey;
    this.toKey = toKey;
    this.transformer = transformer;
    this.valuesTransformer = byteBuffer -> transformer.apply(new AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer>(null, byteBuffer));
    this.keysTransformer = byteBuffer -> transformer.apply(new AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer>(byteBuffer, null));
  }

  public T get(K key) {
    if (key == null)
      throw new NullPointerException();
    if (!externalOakMap.inBounds(key))
      throw new IllegalArgumentException();

    return (T) internalOakMap.getValueTransformation(key, valuesTransformer);
  }

  /**
   * Returns a {@link OakIterator} of a transformation of values contained in this map
   * in ascending order of the corresponding keys.
   */
  public OakIterator<T> valuesIterator() {
    return internalOakMap.valuesTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), valuesTransformer);
  }

  /**
   * Returns a {@link OakIterator} of a transformation of the mappings contained in this map in ascending key order.
   */
  public OakIterator<T> entriesIterator() {
    return internalOakMap.entriesTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), transformer);
  }

  /**
   * Returns a {@link OakIterator} of a transformation of the keys contained in this map in ascending order.
   */
  public OakIterator<T> keysIterator() {
    return internalOakMap.keysTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), keysTransformer);
  }


  @Override public void close() {
    internalOakMap.close();
  }
}
