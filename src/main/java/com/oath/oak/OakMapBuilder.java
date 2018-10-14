/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

/**
 * This class builds a new OakMap instance, and sets serializers, deserializers and allocation size calculators,
 * received from the user.
 *
 * @param <K> The key object type.
 * @param <V> The value object type.
 */
public class OakMapBuilder<K,V> {

  private int MAX_MEM_CAPACITY = Integer.MAX_VALUE; // 2GB per Oak by default

  private OakSerializer<K> keySerializer;
  private OakSerializer<V> valueSerializer;

  private K minKey;

  // comparators
  private OakComparator<K> comparator;

  // Off-heap fields
  private int chunkMaxItems;
  private int chunkBytesPerItem;
  private int memoryCapacity;
  private OakMemoryAllocator memoryAllocator;

  public OakMapBuilder() {
    this.keySerializer = null;
    this.valueSerializer = null;

    this.minKey = null;

    this.comparator = null;

    this.chunkMaxItems = Chunk.MAX_ITEMS_DEFAULT;
    this.chunkBytesPerItem = Chunk.BYTES_PER_ITEM_DEFAULT;
    this.memoryCapacity = MAX_MEM_CAPACITY;
    this.memoryAllocator = null;
  }

  public OakMapBuilder setKeySerializer(OakSerializer<K> keySerializer) {
    this.keySerializer = keySerializer;
    return this;
  }

  public OakMapBuilder setValueSerializer(OakSerializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
    return this;
  }

  public OakMapBuilder setMinKey(K minKey) {
    this.minKey = minKey;
    return this;
  }

  public OakMapBuilder setChunkMaxItems(int chunkMaxItems) {
    this.chunkMaxItems = chunkMaxItems;
    return this;
  }

  public OakMapBuilder setChunkBytesPerItem(int chunkBytesPerItem) {
    this.chunkBytesPerItem = chunkBytesPerItem;
    return this;
  }

  public OakMapBuilder setMemoryCapacity(int memoryCapacity) {
    this.memoryCapacity = memoryCapacity;
    return this;
  }

  public OakMapBuilder setComparator(OakComparator<K> comparator) {
    this.comparator = comparator;
    return this;
  }

  public OakMapBuilder setMemoryAllocator(OakMemoryAllocator ma) {
    this.memoryAllocator = ma;
    return this;
  }

  public OakMap build() {

    MemoryManager memoryManager = (memoryAllocator == null) ?
      new MemoryManager(memoryCapacity, null) :
        new MemoryManager(memoryCapacity, memoryAllocator);

    return new OakMap(
            minKey,
            keySerializer,
            valueSerializer,
            comparator, chunkMaxItems,
            chunkBytesPerItem, memoryManager);
  }

  private static int intsCompare(int int1, int int2) {
    if (int1 > int2)
      return 1;
    else if (int1 < int2)
      return -1;
    return 0;
  }

  public static OakMapBuilder<Integer, Integer> getDefaultBuilder() {

    OakSerializer<Integer> serializerK = new OakSerializer<Integer>() {

      @Override
      public void serialize(Integer obj, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), obj);
      }

      @Override
      public Integer deserialize(ByteBuffer serializedObj) {
        return serializedObj.getInt(serializedObj.position());
      }

      @Override
      public int calculateSize(Integer key) { return Integer.BYTES; }

    };

    OakSerializer<Integer> serializerV = new OakSerializer<Integer>() {

      @Override
      public void serialize(Integer obj, ByteBuffer targetBuffer) {
        assert targetBuffer.position() == 0;
        targetBuffer.putInt(targetBuffer.position(), obj);
      }

      @Override
      public Integer deserialize(ByteBuffer serializedObj) {
        assert serializedObj.position() == 0;
        return serializedObj.getInt(serializedObj.position());
      }

      @Override
      public int calculateSize(Integer key) { return Integer.BYTES; }

    };

    OakComparator<Integer> comparator = new OakComparator<Integer>() {

      @Override
      public int compareKeys(Integer key1, Integer key2) {
        return intsCompare(key1, key2);
      }

      @Override
      public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
        int int1 = serializedKey1.getInt(serializedKey1.position());
        int int2 = serializedKey2.getInt(serializedKey2.position());
        return intsCompare(int1, int2);
      }

      @Override
      public int compareSerializedKeyAndKey(ByteBuffer serializedKey, Integer key) {
        int int1 = serializedKey.getInt(serializedKey.position());
        return intsCompare(int1, key);
      }
    };

    return new OakMapBuilder<Integer, Integer>()
            .setKeySerializer(serializerK)
            .setValueSerializer(serializerV)
            .setMinKey(new Integer(Integer.MIN_VALUE))
            .setComparator(comparator);
  }
}
