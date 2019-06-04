/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

class KeysManager<K> {

    private final MemoryManager memoryManager;
    private final ByteBuf keys;
    private final OakSerializer<K> keySerializer;
    private final Logger log = Logger.getLogger(KeysManager.class.getName());

    KeysManager(int bytes, MemoryManager memoryManager, OakSerializer<K> keySerializer) {
        keys = memoryManager.allocateKeys(bytes);
        keys.writerIndex(keys.capacity());
        this.keySerializer = keySerializer;
        this.memoryManager = memoryManager;
    }

    public int length() {
        return keys.capacity();
    }

    public void writeKey(K key, int ki) {
//        ByteBuf byteBuffer = keys.duplicate();
        ByteBuffer byteBuffer = keys.nioBuffer(ki, keySerializer.calculateSize(key));
//        byteBuffer.position(keys.position() + ki);
//        byteBuffer.limit(keys.position() + ki + keySerializer.calculateSize(key));
//        byteBuffer = byteBuffer.slice();
        // byteBuffer is set so it protects us from the overwrites of the serializer
        keySerializer.serialize(key, byteBuffer);
    }

    public ByteBuf getKeys() {
        return keys;
    }

    public void release() {
        memoryManager.releaseKeys(keys);
    }

    public void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy) {
        //TODO YONIGO - use writerIndex instead of going through bb
        ByteBuf srcKeys = srcKeysManager.getKeys();
        ByteBuffer keysbb = keys.nioBuffer(index, lengthToCopy);
        for (int j = 0; j < lengthToCopy; j++) {
            keysbb.put(j, srcKeys.getByte(srcIndex + j));
        }
    }

//    public int getPosition() {
//        return keys.position();
//    }
}
