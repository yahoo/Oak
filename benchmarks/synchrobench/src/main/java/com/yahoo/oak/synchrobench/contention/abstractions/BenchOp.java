/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The operations that 'CompositionalMap' supports
 */
public enum BenchOp {
    PUT,
    PUT_IF_ABSENT,
    PUT_IF_ABSENT_COMPUTE_IF_PRESENT,
    REMOVE,
    GET,
    ASCEND,
    DESCEND,
    COMPUTE;

    public static BenchOp[] asArray(BenchOp... ops) {
        return ops;
    }

    public BenchOp[] asArray() {
        return asArray(this);
    }

    public static final BenchOp[] ADD = asArray(
        PUT,
        PUT_IF_ABSENT,
        PUT_IF_ABSENT_COMPUTE_IF_PRESENT
    );

    // 'LinkedHashMap' preserves order of insertion.
    public static final Map<String, BenchOp[]> GROUPS = new LinkedHashMap<>();
    static {
        for (BenchOp op : BenchOp.values()) {
            GROUPS.put(op.name(), op.asArray());
        }
        GROUPS.put("Updates", asArray(PUT, PUT_IF_ABSENT, PUT_IF_ABSENT_COMPUTE_IF_PRESENT, REMOVE));
        GROUPS.put("Reads", asArray(GET, ASCEND, DESCEND));
        GROUPS.put("All", BenchOp.values());
    }
}
