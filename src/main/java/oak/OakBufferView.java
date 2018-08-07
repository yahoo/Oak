/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This class allows data retrieval from OakMap while the output is set as OakRBuffer.
 *
 * @param <K> The key object type.
 * @param <V> The value object type.
 */
public class OakBufferView<K,V> {

  private final InternalOakMap internalOakMap;  // to hold the actual data
  private final OakMap externalOakMap;          // to hold the boundaries and memory manager
  private final K fromKey;
  private final K toKey;

  // package level view constructor, to be executed by OakMap only
  OakBufferView(InternalOakMap iOM, OakMap eOM, K fromKey, K toKey) {
    this.internalOakMap = iOM;
    this.externalOakMap = eOM;
    this.fromKey = fromKey;
    this.toKey = toKey;
  }

  OakRBuffer get(K key) {
    if (key == null)
      throw new NullPointerException();
    if (!externalOakMap.inBounds(key))
      throw new IllegalArgumentException();

    externalOakMap.memoryManager.startThread();
    OakRBuffer value = internalOakMap.get(key);
    externalOakMap.memoryManager.stopThread();
    return value;
  }

  /**
   * Returns a {@link CloseableIterator} of the values contained in this map
   * in ascending order of the corresponding keys.
   */
  CloseableIterator<OakRBuffer> valuesIterator() {
    return internalOakMap.valuesBufferViewIterator(
        fromKey, externalOakMap.getFromInclusive(),
        toKey, externalOakMap.getToInclusive(),
        externalOakMap.getIsDescending());
  }

  /**
   * Returns a {@link CloseableIterator} of the mappings contained in this map in ascending key order.
   */
  CloseableIterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesIterator() {
    return internalOakMap.entriesBufferViewIterator(
        fromKey, externalOakMap.getFromInclusive(),
        toKey, externalOakMap.getToInclusive(),
        externalOakMap.getIsDescending());
  }

  /**
   * Returns a {@link CloseableIterator} of the keys contained in this map in ascending order.
   */
  CloseableIterator<OakRBuffer> keysIterator() {
    return internalOakMap.keysBufferViewIterator(
        fromKey, externalOakMap.getFromInclusive(),
        toKey, externalOakMap.getToInclusive(),
        externalOakMap.getIsDescending());
  }
}
