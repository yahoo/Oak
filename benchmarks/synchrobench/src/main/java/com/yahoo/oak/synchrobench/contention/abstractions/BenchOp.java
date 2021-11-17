/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Enumerates the operations that 'CompositionalMap' supports.
 * It is used to identify the current operation and to keep statistic on each operation.
 */
public enum BenchOp {
    PUT,
    PUT_IF_ABSENT,
    PUT_IF_ABSENT_COMPUTE_IF_PRESENT,
    REMOVE,
    GET,
    SCAN_ASCEND,
    SCAN_DESCEND,
    COMPUTE;

    // The operation array/groups below are used to report aggregated statistics on these groups.

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

    /**
     * This group of maps are used for reporting statistics on the operations.
     * We use 'LinkedHashMap' to preserve the order of insertion.
     */
    public static final Map<String, BenchOp[]> GROUPS = new LinkedHashMap<>();
    static {
        for (BenchOp op : BenchOp.values()) {
            GROUPS.put(op.name(), op.asArray());
        }
        GROUPS.put("Updates", asArray(PUT, PUT_IF_ABSENT, PUT_IF_ABSENT_COMPUTE_IF_PRESENT, REMOVE));
        GROUPS.put("Reads", asArray(GET));
        GROUPS.put("Scans", asArray(SCAN_ASCEND, SCAN_DESCEND));
        GROUPS.put("Inserts", asArray(PUT, PUT_IF_ABSENT, PUT_IF_ABSENT_COMPUTE_IF_PRESENT));
        GROUPS.put("All", BenchOp.values());
    }
}
