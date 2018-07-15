/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Consumer;

public class KeysManagerOnHeapImpl<K> extends KeysManager<K> {

    private final byte[] keys;

    KeysManagerOnHeapImpl(int bytes) {
        this.keys = new byte[bytes];
    }

    @Override
    int length() {
        return keys.length;
    }

    @Override
    void writeKey(K key, int ki) {
        throw new UnsupportedOperationException();
    }

    @Override
    ByteBuffer getKeys() {
        return ByteBuffer.wrap(keys);
    }

    @Override
    void release() {
        // do nothing
    }

    @Override
    void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy) {
        // TODO here we assume array
        System.arraycopy(((KeysManagerOnHeapImpl) srcKeysManager).keys, srcIndex, keys, index, lengthToCopy);
    }

    @Override
    int getPosition() {
        return 0;
    }
}
