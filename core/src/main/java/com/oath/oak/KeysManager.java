/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

class KeysManager<K> {

    private final MemoryManager memoryManager;
    private final ByteBuffer keys;
    private final OakSerializer<K> keySerializer;
    private final Logger log = Logger.getLogger(KeysManager.class.getName());

    KeysManager(int bytes, MemoryManager memoryManager, OakSerializer<K> keySerializer) {
        keys = memoryManager.allocateKeys(bytes);
        this.keySerializer = keySerializer;
        this.memoryManager = memoryManager;
    }

    public int length() {
        return keys.remaining();
    }

    public void writeKey(K key, int ki) {
        ByteBuffer byteBuffer = keys.duplicate();
        byteBuffer.position(keys.position() + ki);
        byteBuffer.limit(keys.position() + ki + keySerializer.calculateSize(key));
        byteBuffer = byteBuffer.slice();
        // byteBuffer is set so it protects us from the overwrites of the serializer
        keySerializer.serialize(key, byteBuffer);
    }

    public ByteBuffer getKeys() {
        return keys;
    }

    public void release() {
        memoryManager.releaseKeys(keys);
    }

    public void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy) {

        ByteBuffer srcKeys = srcKeysManager.getKeys();
        int srcKeyPos = srcKeys.position();
        int myPos = keys.position();
        for (int j = 0; j < lengthToCopy; j++) {
            assert(myPos + index + j < keys.limit());
            keys.put(myPos + index + j, srcKeys.get(srcKeyPos + srcIndex + j));
        }
    }

    public int getPosition() {
        return keys.position();
    }
}
