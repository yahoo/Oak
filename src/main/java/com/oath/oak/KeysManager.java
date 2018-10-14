/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

class KeysManager<K> {

    ByteBuffer keys;
    int i;
    MemoryManager memoryManager;
    private final OakSerializer<K> keySerializer;
    AtomicBoolean released;
    Logger log = Logger.getLogger(KeysManager.class.getName());

    KeysManager(int bytes,
                           MemoryManager memoryManager,
                           OakSerializer<K> keySerializer) {
        keys = memoryManager.allocate(bytes);
        assert keys.position() == 0;
        this.memoryManager = memoryManager;
        this.keySerializer = keySerializer;
        this.released = new AtomicBoolean(false);
    }

    public int length() {
        return keys.remaining();
    }

    public void writeKey(K key, int ki) {
        assert keys.position() == 0;
        ByteBuffer byteBuffer = keys.duplicate();
        byteBuffer.position(ki);
        byteBuffer.limit(ki + keySerializer.calculateSize(key));
        byteBuffer = byteBuffer.slice();
        assert byteBuffer.position() == 0;
        // byteBuffer is set so it protects us from the overwrites of the serializer
        keySerializer.serialize(key, byteBuffer);
    }

    public ByteBuffer getKeys() {
        assert keys.position() == 0;
        return keys.duplicate();
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            memoryManager.release(keys);
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
        assert keys.position() == 0;
        return keys.position();
    }
}
