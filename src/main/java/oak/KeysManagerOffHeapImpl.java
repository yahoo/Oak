/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.logging.Logger;
import oak.OakMap.KeyInfo;

public class KeysManagerOffHeapImpl extends KeysManager {

    ByteBuffer keys;
    int i;
    OakMemoryManager memoryManager;
    Logger log = Logger.getLogger(KeysManagerOffHeapImpl.class.getName());

    KeysManagerOffHeapImpl(int bytes, OakMemoryManager memoryManager) {
        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(bytes);
        i = pair.getKey();
        keys = pair.getValue();
        this.memoryManager = memoryManager;
    }

    @Override
    int length() {
        return keys.remaining();
    }

    @Override
    void writeKey(ByteBuffer key, int ki, int length) {
        int keyPos = key.position();
        int myPos = keys.position();
        for (int j = 0; j < length; j++) {
            keys.put(myPos + ki + j, key.get(keyPos + j));
        }
    }

    @Override
    void writeKey(Object key,
                  Consumer<KeyInfo> keyCreator,
                  int ki) {
        keyCreator.accept(new KeyInfo(keys, ki, key));
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
