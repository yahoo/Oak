package com.oath.oak;

/**
 * A sum type for holding either a generic type value or a boolean flag.
 */
class Result<V> {
    final boolean hasValue;     // true if stores value, false if stores flag
    final V value;              // stored value
    final boolean flag;         // stored flag

    static <V> Result<V> withValue(V value) {
        return new Result<>(value, false, true);
    }

    static <V> Result<V> withFlag(boolean flag) {
        return new Result<>(null, flag, false);
    }

    private Result(V value, boolean flag, boolean hasValue) {
        this.value = value;
        this.flag = flag;
        this.hasValue = hasValue;

    }
}
