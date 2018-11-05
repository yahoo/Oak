/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.Map;

/**
 * This class allows data retrieval from OakMap while the output is set as OakRBuffer.
 *
 * @param <K> The key object type.
 */
public class OakBufferView<K> implements AutoCloseable{

  private final InternalOakMap internalOakMap;  // to hold the actual data
  private final OakMap externalOakMap;          // to hold the boundaries and memory manager
  private final K fromKey;
  private final K toKey;

  // package level view constructor, to be executed by OakMap only
  OakBufferView(InternalOakMap iOM, OakMap eOM, K fromKey, K toKey) {
    this.internalOakMap = iOM;
    internalOakMap.open();
    this.externalOakMap = eOM;
    this.fromKey = fromKey;
    this.toKey = toKey;
  }

  public OakRBuffer get(K key) {
    if (key == null)
      throw new NullPointerException();
    if (!externalOakMap.inBounds(key))
      throw new IllegalArgumentException();
    try {
      externalOakMap.getMemoryManager().startOperation();
      return internalOakMap.get(key);
    } finally {
      externalOakMap.getMemoryManager().stopOperation();
    }
  }

  /**
   * Returns a {@link OakCloseableIterator} of the values contained in this map
   * in ascending order of the corresponding keys.
   */
  public OakCloseableIterator<OakRBuffer> valuesIterator() {
    return internalOakMap.valuesBufferViewIterator(
        fromKey, externalOakMap.getFromInclusive(),
        toKey, externalOakMap.getToInclusive(),
        externalOakMap.getIsDescending());
  }

  /**
   * Returns a {@link OakCloseableIterator} of the mappings contained in this map in ascending key order.
   */
  public OakCloseableIterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesIterator() {
    return internalOakMap.entriesBufferViewIterator(
        fromKey, externalOakMap.getFromInclusive(),
        toKey, externalOakMap.getToInclusive(),
        externalOakMap.getIsDescending());
  }

  /**
   * Returns a {@link OakCloseableIterator} of the keys contained in this map in ascending order.
   */
  public OakCloseableIterator<OakRBuffer> keysIterator() {
    return internalOakMap.keysBufferViewIterator(
        fromKey, externalOakMap.getFromInclusive(),
        toKey, externalOakMap.getToInclusive(),
        externalOakMap.getIsDescending());
  }


  @Override
  public void close() {
    internalOakMap.close();
  }
}
