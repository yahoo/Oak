/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * A sum type for holding either a generic type value or a boolean flag.
 */
class Result {
    // This value holds whether the value succeeded or not, and whether the relevant value was moved.
    ValueUtils.ValueResult operationResult;
    // In case of success, the result value is set in value.
    Object value;

    Result() {
        invalidate();
    }

    void invalidate() {
        this.operationResult = ValueUtils.ValueResult.FALSE;
        this.value = null;
    }

    Result withValue(Object value) {
        this.operationResult = ValueUtils.ValueResult.TRUE;
        this.value = value;
        return this;
    }

    Result withFlag(ValueUtils.ValueResult flag) {
        this.operationResult = flag;
        this.value = null;
        return this;
    }

    Result withFlag(boolean flag) {
        this.operationResult = flag ? ValueUtils.ValueResult.TRUE : ValueUtils.ValueResult.FALSE;
        this.value = null;
        return this;
    }
}
