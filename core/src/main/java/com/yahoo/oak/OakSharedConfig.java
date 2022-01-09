/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A shared configuration instance that can be passed between Oak instances and its internal classes to avoid passing
 * all these parameters one by one.
 */
public class OakSharedConfig<K, V> {
    public final BlockMemoryAllocator memoryAllocator;

    public final MemoryManager keysMemoryManager;
    public final MemoryManager valuesMemoryManager;

    public final OakSerializer<K> keySerializer;
    public final OakSerializer<V> valueSerializer;
    public final OakComparator<K> comparator;

    public final ValueUtils valueOperator;

    public final AtomicInteger size;
    
    protected boolean prtKeys;

    public OakSharedConfig(
            BlockMemoryAllocator memoryAllocator,
            MemoryManager keysMemoryManager,
            MemoryManager valuesMemoryManager,
            OakSerializer<K> keySerializer,
            OakSerializer<V> valueSerializer,
            OakComparator<K> comparator
    ) {
        this.memoryAllocator = memoryAllocator;
        this.valuesMemoryManager = valuesMemoryManager;
        this.keysMemoryManager = keysMemoryManager;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.comparator = comparator;
        this.valueOperator = new ValueUtils();
        this.size = new AtomicInteger(0);
        this.prtKeys = false; 
    }
}
