package com.yahoo.oak;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class InternalOakHashMap<K,V> implements Closeable {
    public InternalOakHashMap(MemoryManager keysMemoryManager, MemoryManager valuesMemoryManager) {
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public V get(K key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public V getValueTransformation(K key, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");

    }

    public Result putIfAbsent(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    <T> Iterator<T> entriesTransformIterator(Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>,T> transformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public <K, V> void foreach(BiConsumer<? super K,? super V> action) {
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

    public Result put(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public <K, V> void replaceAll(BiFunction<? super K,? super V,? extends V> function, OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public void clear() {
        throw new UnsupportedOperationException("Not implemented yet");

    }

    public int size() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<V> valuesTransformIterator(OakTransformer<V> valueDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Iterator<K> keysTransformIterator(OakTransformer<K> keyDeserializeTransformer) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
