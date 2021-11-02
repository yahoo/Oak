/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

/**
 * Extends 'Comparable' for ConcurrentSkipListMap.
 */
public interface BenchKey extends Comparable<Object> {
    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();
}
