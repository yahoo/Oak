package com.yahoo.oak;

import java.io.Closeable;
import java.io.IOException;

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
}
