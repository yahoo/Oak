/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.eventcache;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;

public class Value implements BenchValue {
    float val1;
    float val2;
    int val3;

    public Value() {
        this(0.f, 0.f, 0);
    }

    public Value(float val1, float val2, int val3) {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
    }

    public float getVal1() {
        return val1;
    }

    public float getVal2() {
        return val2;
    }

    public int getVal3() {
        return val3;
    }

    public void setVal3(int val3) {
        this.val3 = val3;
    }

}
