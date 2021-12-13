/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

/**
 * All benchmarked keys should implement this interface.
 * It is used for strict typing purposes, but also to force the implementation of methods that required by
 * on-heap maps.
 * I.e., it extends 'Comparable', and require the implementation of hashCode().
 */
public interface BenchKey extends Comparable<Object> {
    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();
}
