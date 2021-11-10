/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class IncreasingKeyIndex {

    private static final AtomicInteger CUR_INDEX = new AtomicInteger();

    public static void reset() {
        CUR_INDEX.set(0);
    }

    public static IntStream ints(int bound) {
        return IntStream.generate(CUR_INDEX::getAndIncrement).map(i -> i % bound);
    }
}
