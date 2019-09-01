package com.oath.oak;

class Result<V> {
    final V value;
    final boolean flag;
    final boolean hasValue;

    private Result(V value, boolean flag, boolean hasValue) {
        this.value = value;
        this.flag = flag;
        this.hasValue = hasValue;

    }

    static <V> Result<V> withValue(V value) {
        return new Result<>(value, false, true);
    }

    static <V> Result<V> withFlag(boolean flag) {
        return new Result<>(null, flag, false);
    }

}
