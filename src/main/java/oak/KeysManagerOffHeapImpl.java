/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class KeysManagerOffHeapImpl<K> extends KeysManager<K> {

    ByteBuffer keys;
    int i;
    OakMemoryManager memoryManager;
    private final Serializer<K> keySerializer;
    private final SizeCalculator<K> keySizeCalculator;
    Logger log = Logger.getLogger(KeysManagerOffHeapImpl.class.getName());

    KeysManagerOffHeapImpl(int bytes, OakMemoryManager memoryManager,
                           Serializer<K> keySerializer, SizeCalculator<K> keySizeCalculator) {
        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(bytes);
        i = pair.getKey();
        keys = pair.getValue();
        this.memoryManager = memoryManager;
        this.keySerializer = keySerializer;
        this.keySizeCalculator = keySizeCalculator;
    }

    @Override
    int length() {
        return keys.remaining();
    }

    @Override
    void writeKey(K key, int ki) {
        ByteBuffer byteBuffer = keys.duplicate();
        byteBuffer.position(keys.position() + ki);
        keySerializer.serialize(key, byteBuffer);
    }

    @Override
    ByteBuffer getKeys() {
        return keys;
    }

    @Override
    void release() {
        memoryManager.release(i, keys);
    }

    @Override
    void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy) {
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

    @Override
    int getPosition() {
        return keys.position();
    }
}
