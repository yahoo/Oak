/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * This class builds a new OakMap instance, and sets serializers, deserializers and allocation size calculators,
 * received from the user.
 *
 * @param <K> The key object type.
 * @param <V> The value object type.
 */
public class OakMapBuilder<K, V> {

    private static final long MAX_MEM_CAPACITY = ((long) Integer.MAX_VALUE) * 8; // 16GB per Oak by default

    private OakSerializer<K> keySerializer;
    private OakSerializer<V> valueSerializer;

    private K minKey;

    // comparators
    private OakComparator<K> comparator;

    // configure number of chunks and items inside them
    private int orderedChunkMaxItems; // to be used for creating OakMap
    private int hashChunkMaxItems; // to be used for creating OakHash
    private int preallocHashChunksNum; // to be used for creating OakHash

    // Off-heap fields
    private long memoryCapacity;
    private BlockMemoryAllocator memoryAllocator;
    private Integer preferredBlockSizeBytes;

    public OakMapBuilder(OakComparator<K> comparator,
                         OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer, K minKey) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.minKey = minKey;

        this.comparator = comparator;

        this.orderedChunkMaxItems = OrderedChunk.ORDERED_CHUNK_MAX_ITEMS_DEFAULT;
        this.hashChunkMaxItems = HashChunk.HASH_CHUNK_MAX_ITEMS_DEFAULT;
        this.preallocHashChunksNum = FirstLevelHashArray.HASH_CHUNK_NUM_DEFAULT;
        this.memoryCapacity = MAX_MEM_CAPACITY;
        this.memoryAllocator = null;
        this.preferredBlockSizeBytes = null;
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

    public OakMapBuilder<K, V> setOrderedChunkMaxItems(int orderedChunkMaxItems) {
        this.orderedChunkMaxItems = orderedChunkMaxItems;
        return this;
    }

    public OakMapBuilder<K, V> setHashChunkMaxItems(int hashChunkMaxItems) {
        this.hashChunkMaxItems = hashChunkMaxItems;
        return this;
    }

    public OakMapBuilder<K, V> setPreallocHashChunksNum(int preallocHashChunksNum) {
        this.preallocHashChunksNum = preallocHashChunksNum;
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

    public OakMapBuilder<K, V> setMemoryAllocator(BlockMemoryAllocator ma) {
        this.memoryAllocator = ma;
        return this;
    }

    /**
     * Sets the preferred block size. This only has an effect if OakMap was never instantiated before.
     * @param preferredBlockSizeBytes the preferred block size
     */
    public OakMapBuilder<K, V> setPreferredBlockSize(int preferredBlockSizeBytes) {
        this.preferredBlockSizeBytes = preferredBlockSizeBytes;
        return this;
    }


    private void checkPreconditions() {
        if (comparator == null) {
            throw new IllegalStateException("Must provide a non-null comparator to build the Oak");
        }
        if (keySerializer == null) {
            throw new IllegalStateException("Must provide a non-null key serializer to build the Oak");
        }
        if (valueSerializer == null) {
            throw new IllegalStateException("Must provide a non-null value serializer to build the Oak");
        }
    }


    public OakMap<K, V> buildOrderedMap() {
        if (preferredBlockSizeBytes != null) {
            BlocksPool.preferBlockSize(preferredBlockSizeBytes);
        }

        if (memoryAllocator == null) {
            this.memoryAllocator = new NativeMemoryAllocator(memoryCapacity);
        }

        MemoryManager valuesMemoryManager = new SyncRecycleMemoryManager(memoryAllocator);
        KeyMemoryManager keysMemoryManager = new SeqExpandMemoryManager(memoryAllocator);
        checkPreconditions();
        if (minKey == null) {
            throw new IllegalStateException("Must provide a non-null minimal key object to build the OakMap");
        }
        return new OakMap<>(
                minKey,
                keySerializer,
                valueSerializer,
                comparator, orderedChunkMaxItems,
                valuesMemoryManager, keysMemoryManager);
    }





    public OakHashMap<K, V> buildHashMap() {
        if (preferredBlockSizeBytes != null) {
            BlocksPool.preferBlockSize(preferredBlockSizeBytes);
        }

        if (memoryAllocator == null) {
            this.memoryAllocator = new NativeMemoryAllocator(memoryCapacity);
        }
        MemoryManager valuesMemoryManager = new SyncRecycleMemoryManager(memoryAllocator);
        // for hash the keys are indeed deleted, thus SeqExpandMemoryManager isn't acceptable
        KeyMemoryManager keysMemoryManager = new SyncRecycleMemoryManager(memoryAllocator);

        // Number of bits to define the chunk size is calculated from given number of items
        // to be kept in one hash chunk. The number of chunks pre-allocated in the hash is
        // configurable and also passes in the bit size
        int bitsToKeepChunkSize = (int) Math.ceil(Math.log(hashChunkMaxItems) / Math.log(2));
        int bitsToKeepChunksNum = (int) Math.ceil(Math.log(preallocHashChunksNum) / Math.log(2));

        System.gc(); // the below is memory costly, be sure all unreachable memory is cleared

        checkPreconditions();
        return new OakHashMap<>(keySerializer, valueSerializer,
                comparator,
                bitsToKeepChunkSize, bitsToKeepChunksNum, valuesMemoryManager, keysMemoryManager);
    }

}
