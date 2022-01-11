/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

public interface ConcurrentZCMap<K , V> extends ConcurrentMap<K , V>, Closeable {
    ZeroCopyMap<K , V> zc();
    long memorySize();
    void close();
}
