/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;

/**
 * This class builds a new OakMap instance, and sets serializers, deserializers and allocation size calculators,
 * received from the user.
 *
 * @param <K> The key object type.
 * @param <V> The value object type.
 */
public class OakMapBuilder<K,V> {

  private KeySerializer<K> keySerializer;
  private SizeCalculator<K> keySizeCalculator;
  private ValueSerializer<K, V> valueSerializer;
  private SizeCalculator<V> valueSizeCalculator;

  private K minKey;

  // comparators
  private OakComparator<K,K> keysComparator;
  private OakComparator<ByteBuffer,ByteBuffer> serializationsComparator;
  private OakComparator<ByteBuffer,K> serializationAndKeyComparator;

  // Off-heap fields
  private int chunkMaxItems;
  private int chunkBytesPerItem;
  private MemoryPool memoryPool;

  public OakMapBuilder() {
    this.keySerializer = null;
    this.keySizeCalculator = null;
    this.valueSerializer = null;
    this.valueSizeCalculator = null;

    this.minKey = null;

    this.keysComparator = null;
    this.serializationsComparator = null;
    this.serializationAndKeyComparator = null;

    this.chunkMaxItems = Chunk.MAX_ITEMS_DEFAULT;
    this.chunkBytesPerItem = Chunk.BYTES_PER_ITEM_DEFAULT;
    this.memoryPool = new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE);
  }

  public OakMapBuilder setKeySerializer(KeySerializer<K> keySerializer) {
    this.keySerializer = keySerializer;
    return this;
  }

  public OakMapBuilder setKeySizeCalculator(SizeCalculator<K> keySizeCalculator) {
    this.keySizeCalculator = keySizeCalculator;
    return this;
  }

  public OakMapBuilder setValueSerializer(ValueSerializer<K, V> valueSerializer) {
    this.valueSerializer = valueSerializer;
    return this;
  }

  public OakMapBuilder setValueSizeCalculator(SizeCalculator<V> valueSizeCalculator) {
    this.valueSizeCalculator = valueSizeCalculator;
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

  public OakMapBuilder setMemoryPool(MemoryPool memoryPool) {
    this.memoryPool = memoryPool;
    return this;
  }

  public OakMapBuilder setKeysComparator(OakComparator<K,K> keysComparator) {
    this.keysComparator = keysComparator;
    return this;
  }

  public OakMapBuilder setSerializationsComparator(OakComparator<ByteBuffer,ByteBuffer> serializationsComparator) {
    this.serializationsComparator = serializationsComparator;
    return this;
  }

  public OakMapBuilder setSerializationAndKeyComparator(OakComparator<ByteBuffer,K> serializationAndKeyComparator) {
    this.serializationAndKeyComparator = serializationAndKeyComparator;
    return this;
  }

  public OakMapOffHeapImpl buildOffHeapOakMap() {

    assert this.keySerializer != null;
    assert this.keySizeCalculator != null;
    assert this.valueSerializer != null;
    assert this.valueSizeCalculator != null;
    assert this.minKey != null;
    assert this.keysComparator != null;
    assert this.serializationsComparator != null;
    assert this.serializationAndKeyComparator != null;

    return new OakMapOffHeapImpl(
            minKey,
            keySerializer,
            keySizeCalculator,
            valueSerializer,
            valueSizeCalculator,
            keysComparator,
            serializationsComparator,
            serializationAndKeyComparator,
            memoryPool,
            chunkMaxItems,
            chunkBytesPerItem);
  }

  private static int intsCompare(int int1, int int2) {
    if (int1 > int2)
      return 1;
    else if (int1 < int2)
      return -1;
    return 0;
  }

  public static OakMapBuilder<Integer, Integer> getDefaultBuilder() {

    KeySerializer<Integer> keySerializer = new KeySerializer<Integer>() {

      @Override
      public void serialize(Integer key, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), key);
      }

      @Override
      public Integer deserialize(ByteBuffer serializedKey) {
        return serializedKey.getInt(serializedKey.position());
      }

    };

    SizeCalculator<Integer> sizeCalculator = new SizeCalculator<Integer>() {
      @Override
      public int calculateSize(Integer object) {
        return Integer.BYTES;
      }
    };

    ValueSerializer<Integer, Integer> valueSerializer = new ValueSerializer<Integer, Integer>() {
      @Override
      public void serialize(Integer key, Integer value, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), value);
      }

      @Override
      public Integer deserialize(ByteBuffer serializedKey, ByteBuffer serializedValue) {
        return serializedValue.getInt(serializedValue.position());
      }
    };

    OakComparator<Integer, Integer> keysComparator = new OakComparator<Integer, Integer>() {
      @Override
      public int compare(Integer int1, Integer int2) {
        return intsCompare(int1, int2);
      }
    };

    OakComparator<ByteBuffer, ByteBuffer> serializationsComparator = new OakComparator<ByteBuffer, ByteBuffer>() {
      @Override
      public int compare(ByteBuffer buff1, ByteBuffer buff2) {
        int int1 = buff1.getInt(buff1.position());
        int int2 = buff2.getInt(buff2.position());
        return intsCompare(int1, int2);
      }
    };

    OakComparator<ByteBuffer, Integer> serializationAndKeyComparator = new OakComparator<ByteBuffer, Integer>() {
      @Override
      public int compare(ByteBuffer buff1, Integer int2) {
        int int1 = buff1.getInt(buff1.position());
        return intsCompare(int1, int2);
      }
    };

    return new OakMapBuilder<Integer, Integer>()
            .setKeySerializer(keySerializer)
            .setKeySizeCalculator(sizeCalculator)
            .setValueSerializer(valueSerializer)
            .setValueSizeCalculator(sizeCalculator)
            .setMinKey(new Integer(Integer.MIN_VALUE))
            .setKeysComparator(keysComparator)
            .setSerializationsComparator(serializationsComparator)
            .setSerializationAndKeyComparator(serializationAndKeyComparator);
  }
}
