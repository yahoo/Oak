/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class KeysManager<K> {

    abstract int length();

    abstract void writeKey(K key, int ki);

    abstract ByteBuffer getKeys();

    abstract void release();

    abstract void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy);

    abstract int getPosition();

}
