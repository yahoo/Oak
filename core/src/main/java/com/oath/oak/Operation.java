/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

// this enum class describes the operation the thread is progressing with. The enum is also used
// for helping in rebalance time
public enum Operation {
    NO_OP,
    PUT,
    PUT_IF_ABSENT,
    REMOVE,
    COMPUTE,
    PUT_IF_ABS_COMPUTE_IF_PRES
}
