/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

class KeysManagerOffHeapImpl<K> implements KeysManager<K> {

    ByteBuffer keys;
    int i;
    OakMemoryManager memoryManager;
    private final Serializer<K> keySerializer;
    AtomicBoolean released;
    Logger log = Logger.getLogger(KeysManagerOffHeapImpl.class.getName());

    KeysManagerOffHeapImpl(int bytes,
                           OakMemoryManager memoryManager,
                           Serializer<K> keySerializer) {
        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(bytes);
        i = pair.getKey();
        keys = pair.getValue();
        this.memoryManager = memoryManager;
        this.keySerializer = keySerializer;
        this.released = new AtomicBoolean(false);
    }

    public int length() {
        return keys.remaining();
    }

    public void writeKey(K key, int ki) {
        ByteBuffer byteBuffer = keys.duplicate();
        byteBuffer.position(keys.position() + ki);
        keySerializer.serialize(key, byteBuffer);
    }

    public ByteBuffer getKeys() {
        return keys;
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            memoryManager.release(i, keys);
        }
    }

    public void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy) {
        ByteBuffer srcKeys = srcKeysManager.getKeys();
        int srcKeyPos = srcKeys.position();
        int myPos = keys.position();
        for (int j = 0; j < lengthToCopy; j++) {
            if (myPos + index + j >= keys.limit()) {
                log.info("can't put in buffer..");
            }
            keys.put(myPos + index + j, srcKeys.get(srcKeyPos + srcIndex + j));
        }
    }

    public int getPosition() {
        return keys.position();
    }
}
