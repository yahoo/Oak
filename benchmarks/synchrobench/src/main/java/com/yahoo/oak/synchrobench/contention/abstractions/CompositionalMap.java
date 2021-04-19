/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import java.util.Map;

/**
 * Compositional map interface
 *
 * @author Vincent Gramoli
 */
public interface CompositionalMap<K, V> extends Map<K, V> {

    V putIfAbsent(K k, V v);

    void clear();

    int size();
}
