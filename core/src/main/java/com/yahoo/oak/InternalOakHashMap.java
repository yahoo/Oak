/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class InternalOakHashMap<K , V> implements Closeable {
    private final MemoryManager valuesMemoryManager;
    private final MemoryManager keysMemoryManager;
    private final AtomicInteger size;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    private final OakHashFunction<K , ?> hashFunction;

    public InternalOakHashMap(MemoryManager valuesMemoryManager,
                              MemoryManager keysMemoryManager,
                              OakSerializer<K> keySerializer,
                              OakSerializer<V> valueSerializer,
                              OakHashFunction<K , ? extends Number> hashFunction) {
        this.size = new AtomicInteger(0);
        this.valuesMemoryManager = valuesMemoryManager;
        this.keysMemoryManager = keysMemoryManager;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.hashFunction = hashFunction;
    }


    @Override
    public void close()  {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    private <T> T getValueTransformation(OakScopedReadBuffer key, OakTransformer<T> transformer) {
        K deserializedKey = this.keySerializer.deserialize(key);
        return getValueTransformation(deserializedKey, transformer);
    }

    // the non-ZC variation of the get
    <T> T getValueTransformation(K key, OakTransformer<T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Result putIfAbsent(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    <T> Iterator<T> entriesTransformIterator(
            Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer> , T> transformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public <K, V> void foreach(BiConsumer<? super K , ? super V> action) {
        throw new UnsupportedOperationException("Not implemented yet");

    }

    public Result remove(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public V replace(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public boolean replace(K key, V oldValue, V newValue, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public V put(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public <K, V> void replaceAll(BiFunction<? super K , ? super V , ? extends V> function,
                                  OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    public boolean computeIfPresent(K key, Consumer<OakScopedWriteBuffer> computer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public void clear() {
        throw new UnsupportedOperationException("Not implemented yet");

    }

    public int size() {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    public long memorySize() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<V> valuesTransformIterator(OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<K> keysTransformIterator(OakTransformer<K> keyDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public OakUnscopedBuffer get(K key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakScopedWriteBuffer> computer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<OakUnscopedBuffer> keysBufferViewIterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<OakUnscopedBuffer> valuesBufferViewIterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesBufferViewIterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<OakUnscopedBuffer> keysStreamIterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<OakUnscopedBuffer> valuesStreamIterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesStreamIterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }



}
