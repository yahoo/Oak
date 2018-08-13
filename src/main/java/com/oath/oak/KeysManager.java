/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

interface KeysManager<K> {

    int length();

    void writeKey(K key, int ki);

    ByteBuffer getKeys();

    // Shuold be thread-safe
    void release();

    void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy);

    int getPosition();

}
