package com.oath.oak;

import static com.oath.oak.ValueUtils.ValueResult.FALSE;
import static com.oath.oak.ValueUtils.ValueResult.TRUE;

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
        this.operationResult = FALSE;
        this.value = null;
    }

    Result withValue(Object value) {
        this.operationResult = TRUE;
        this.value = value;
        return this;
    }

    Result withFlag(ValueUtils.ValueResult flag) {
        this.operationResult = flag;
        this.value = null;
        return this;
    }
}
