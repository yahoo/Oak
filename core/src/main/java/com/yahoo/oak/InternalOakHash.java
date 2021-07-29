/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

class InternalOakHash<K, V> {

    /*-------------- Members --------------*/
    private final FirstLevelHashArray<K, V> hashArray;    // FirstLevelHashArray
    private final OakComparator<K> comparator;
    private final MemoryManager valuesMemoryManager;
    private final MemoryManager keysMemoryManager;
    private final AtomicInteger size;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    private final ValueUtils valueOperator;
    static final int MAX_RETRIES = 1024;
    private static final int MOST_SIGN_BITS_NUM = 16;

    /*-------------- Constructors --------------*/
    InternalOakHash(K minKey, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
        OakComparator<K> oakComparator, MemoryManager vMM, MemoryManager kMM, int chunkMaxItems,
        ValueUtils valueOperator) {

        this.size = new AtomicInteger(0);
        this.valuesMemoryManager = vMM;
        this.keysMemoryManager = kMM;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.comparator = oakComparator;
        this.valueOperator = valueOperator;

        this.hashArray = new FirstLevelHashArray(MOST_SIGN_BITS_NUM, this.size, vMM, kMM, oakComparator,
            keySerializer, valueSerializer);

    }

    /*-------------- Closable --------------*/
    /**
     * cleans off heap memory
     */
    void close() {
        try {
            // closing the same memory manager (or memory allocator) twice,
            // has the same effect as closing once
            valuesMemoryManager.close();
            keysMemoryManager.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*-------------- size --------------*/
    /**
     * @return current off heap memory usage in bytes
     */
    long memorySize() {
        if (valuesMemoryManager != keysMemoryManager) {
            // if two memory managers are not the same instance
            return valuesMemoryManager.allocated() + keysMemoryManager.allocated();
        }
        return valuesMemoryManager.allocated();
    }
    int entries() {
        return size.get();
    }

    /*-------------- Context --------------*/
    /**
     * Should only be called from API methods at the beginning of the method and be reused in internal calls.
     *
     * @return a context instance.
     */
    ThreadContext getThreadContext() {
        return new ThreadContext(keysMemoryManager, valuesMemoryManager);
    }

    /**
     * @param c - OrderedChunk to rebalance
     */
    private void rebalance(HashChunk<K, V> c) {
        //TODO: to be implemented
        return;
    }

    private void checkRebalance(HashChunk<K, V> c) {
        if (c.shouldRebalance()) {
            rebalance(c);
        }
    }
}
