/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

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

public class OakTransformView<K, T> {

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
    this.externalOakMap = eOM;
    this.fromKey = fromKey;
    this.toKey = toKey;
    this.transformer = transformer;
    this.valuesTransformer = new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer byteBuffer) {
        return transformer.apply(new AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer>(null, byteBuffer));
      }
    };
    this.keysTransformer = new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer byteBuffer) {
        return transformer.apply(new AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer>(byteBuffer, null));
      }
    };
  }

  T get(K key) {
    if (key == null)
      throw new NullPointerException();
    if (!externalOakMap.inBounds(key))
      throw new IllegalArgumentException();

    externalOakMap.memoryManager.startThread();
    T transformation = (T) internalOakMap.getValueTransformation(key, transformer);
    externalOakMap.memoryManager.stopThread();
    return transformation;
  }

  /**
   * Returns a {@link CloseableIterator} of a transformation of values contained in this map
   * in ascending order of the corresponding keys.
   */
  CloseableIterator<T> valuesIterator() {
    return internalOakMap.valuesTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), valuesTransformer);
  }

  /**
   * Returns a {@link CloseableIterator} of a transformation of the mappings contained in this map in ascending key order.
   */
  CloseableIterator<T> entriesIterator() {
    return internalOakMap.entriesTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), transformer);
  }

  /**
   * Returns a {@link CloseableIterator} of a transformation of the keys contained in this map in ascending order.
   */
  CloseableIterator<T> keysIterator() {
    return internalOakMap.keysTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), keysTransformer);
  }


}
