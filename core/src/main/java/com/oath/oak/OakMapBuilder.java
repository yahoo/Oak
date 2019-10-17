/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;

/**
 * This class builds a new OakMap instance, and sets serializers, deserializers and allocation size calculators,
 * received from the user.
 *
 * @param <K> The key object type.
 * @param <V> The value object type.
 */
public class OakMapBuilder<K, V> {

    private final long MAX_MEM_CAPACITY = ((long) Integer.MAX_VALUE) * 8; // 16GB per Oak by default

    private OakSerializer<K> keySerializer;
    private OakSerializer<V> valueSerializer;

    private K minKey;

    // comparators
    private OakComparator<K> comparator;

    // Off-heap fields
    private int chunkMaxItems;
    private long memoryCapacity;
    private OakBlockMemoryAllocator memoryAllocator;
    private NovaValueOperations operator;

    public OakMapBuilder() {
        this.keySerializer = null;
        this.valueSerializer = null;

        this.minKey = null;

        this.comparator = null;

        this.chunkMaxItems = Chunk.MAX_ITEMS_DEFAULT;
        this.memoryCapacity = MAX_MEM_CAPACITY;
        this.memoryAllocator = null;
        this.operator = new NovaValueOperationsImpl();
    }

    public OakMapBuilder<K, V> setKeySerializer(OakSerializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public OakMapBuilder<K, V> setValueSerializer(OakSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public OakMapBuilder<K, V> setMinKey(K minKey) {
        this.minKey = minKey;
        return this;
    }

    public OakMapBuilder<K, V> setChunkMaxItems(int chunkMaxItems) {
        this.chunkMaxItems = chunkMaxItems;
        return this;
    }

    public OakMapBuilder<K, V> setMemoryCapacity(long memoryCapacity) {
        this.memoryCapacity = memoryCapacity;
        return this;
    }

    public OakMapBuilder<K, V> setComparator(OakComparator<K> comparator) {
        this.comparator = comparator;
        return this;
    }

    public OakMapBuilder<K, V> setMemoryAllocator(OakBlockMemoryAllocator ma) {
        this.memoryAllocator = ma;
        return this;
    }

    public OakMap<K, V> build() {

        if (memoryAllocator == null) {
            this.memoryAllocator = new OakNativeMemoryAllocator(memoryCapacity);
        }

        NovaManager memoryManager = new NovaManager((OakNativeMemoryAllocator) memoryAllocator);

        return new OakMap<>(
                minKey,
                keySerializer,
                valueSerializer,
                comparator, chunkMaxItems,
                memoryManager, operator);
    }

}
