/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.eventcache;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;

public class Value implements BenchValue {
    float field1;
    float field2;
    int field3;

    public Value() {
        this(0.f, 0.f, 0);
    }

    public Value(float field1, float field2, int field3) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
    }

    public float getField1() {
        return field1;
    }

    public float getField2() {
        return field2;
    }

    public int getField3() {
        return field3;
    }

    public void setField3(int field3) {
        this.field3 = field3;
    }

}
