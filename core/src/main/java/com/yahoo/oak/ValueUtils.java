/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.function.Consumer;

class ValueUtils {

    enum ValueResult {
        TRUE, FALSE, RETRY
    }

    /* ==================== Methods for operating on existing off-heap values ==================== */

    /**
     * Used to try and read a value off-heap
     *
     * @param result      The result object
     * @param value       the value's off-heap Slice object
     * @param transformer value deserializer
     * @param <T>         the type of {@code transformer}'s output
     * @return {@code TRUE} if the value was read successfully
     * {@code FALSE} if the value is deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     * In case of {@code TRUE}, the read value is stored in the returned Result, otherwise, the value is {@code null}.
     */
    <T> Result transform(Result result, ValueBuffer value, OakTransformer<T> transformer) {
        ValueResult ret = value.s.lockRead();
        if (ret != ValueResult.TRUE) {
            return result.withFlag(ret);
        }

        try {
            T transformation = transformer.apply(value);
            return result.withValue(transformation);
        } finally {
            value.s.unlockRead();
        }
    }

    /**
     * @see #exchange(Chunk, ThreadContext, Object, OakTransformer, OakSerializer, InternalOakMap)
     * Does not return the value previously written off-heap
     */
    <V> ValueResult put(Chunk<?, V> chunk, ThreadContext ctx, V newVal, OakSerializer<V> serializer,
        InternalOakMap internalOakMap) {

        ValueResult result = ctx.value.s.lockWrite();
        if (result != ValueResult.TRUE) {
            return result;
        }
        result = innerPut(chunk, ctx, newVal, serializer, internalOakMap);
        // in case move happened: ctx.valueSlice might be set to a new slice.
        // Alternatively, if returned result is RETRY, a rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        ctx.value.s.unlockWrite();
        return result;
    }

    private <V> ValueResult innerPut(Chunk<?, V> chunk, ThreadContext ctx, V newVal, OakSerializer<V> serializer,
        InternalOakMap internalOakMap) {
        int capacity = serializer.calculateSize(newVal);
        if (capacity > ctx.value.getLength()) {
            return moveValue(chunk, ctx, internalOakMap, newVal);
        }
        ScopedWriteBuffer.serialize(ctx.value.s, newVal, serializer);
        return ValueResult.TRUE;
    }

    private <V> ValueResult moveValue(
        Chunk<?, V> chunk, ThreadContext ctx, InternalOakMap internalOakMap, V newVal) {

        boolean moved = internalOakMap.overwriteExistingValueForMove(ctx, newVal, chunk);
        if (!moved) {
            // rebalance was needed or the entry was updated by someone else, need to retry
            return ValueResult.RETRY;
        }
        // The value was moved, the header of the old slice needs to me marked as moved
        // Couldn't release the write lock on the old slice or mark it as moved, before the new one is updated!
        // Marking the old slide as moved now, the write lock is still held
        ctx.value.s.markAsMoved();
        // currently the slices which value was moved aren't going to be released, to keep the MOVED mark
        // They need to be released when the target slice is released (moved to free list)
        // TODO: deal with the reallocation of the moved memory

        ctx.value.copyFrom(ctx.newValue);
        return ValueResult.TRUE;
    }

    /**
     * @param value    the value's off-heap Slice object
     * @param computer the function to apply on the Slice
     * @return {@code TRUE} if the function was applied successfully,
     * {@code FAILURE} if the value is deleted,
     * {@code RETRY} if the value was moved.
     */
    ValueResult compute(ValueBuffer value, Consumer<OakScopedWriteBuffer> computer) {
        ValueResult result = value.s.lockWrite();
        if (result != ValueResult.TRUE) {
            return result;
        }

        try {
            ScopedWriteBuffer.compute(value.s, computer);
        } finally {
            value.s.unlockWrite();
        }

        return ValueResult.TRUE;
    }

    /**
     * Marks a value as deleted and frees its slice (whether the header is freed or not is implementation dependant).
     *
     * @param <V>           the type of the value
     * @param ctx           has the entry index and its value to be remove
     * @param oldValue      in case of a conditional remove, this is the value to which the actual value is compared to
     * @param transformer   value deserializer
     * @return {@code TRUE} if the value was removed successfully,
     * {@code FALSE} if the value is already deleted, or if it does not equal to {@code oldValue}
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     * In case of success, the value of the returned Result is the value which was written in the off-heap before the
     * removal (if {@code transformer} is not null), otherwise, it is {@code null}.
     */
    <V> Result remove(ThreadContext ctx, V oldValue, OakTransformer<V> transformer) {
        // Not a conditional remove, so we can delete immediately
        if (oldValue == null) {
            // try to delete
            ValueResult result = ctx.value.s.logicalDelete();
            if (result != ValueResult.TRUE) {
                return ctx.result.withFlag(result);
            }
            // Now the value is deleted, and all other threads will treat it as deleted,
            // but it is not yet freed, so this thread can read from it.
            // read the old value (the slice is not reclaimed yet)
            V v = transformer != null ? transformer.apply(ctx.value) : null;
            // return TRUE with the old value
            return ctx.result.withValue(v);
        } else {
            // This is a conditional remove, so we first have to check whether the current value matches the expected
            // one.
            // We start by acquiring a write lock for reading since we do not want concurrent reads.
            ValueResult result = ctx.value.s.lockWrite();
            if (result != ValueResult.TRUE) {
                return ctx.result.withFlag(result);
            }
            V v = transformer.apply(ctx.value);
            // This is where we check the equality between the expected value and the actual value
            if (!oldValue.equals(v)) {
                ctx.value.s.unlockWrite();
                return ctx.result.withFlag(ValueResult.FALSE);
            }
            // both values match so the value is marked as deleted.
            // No need for a CAS since a write lock is exclusive
            ctx.value.s.markAsDeleted();
            // delete the value in the entry happens next and the slice will be released as part of it
            // slice can be released only after the entry is marked appropriately
            return ctx.result.withValue(v);
        }
    }

    /**
     * Replaces the value written in the Slice referenced by {@code ctx} with {@code value}.
     * {@code chuck} is used iff {@code newValue} takes more space than the old value does, meaning it has to move.
     * If the value moves, the old slice is marked as moved and freed.
     *
     * @param <V>                         the type of the value
     * @param chunk                       the chunk with the entry to which the value is linked to
     * @param ctx                         has the entry index and its value
     * @param value                       the new value to write
     * @param valueDeserializeTransformer used to read the previous value
     * @param serializer                  value serializer to write {@code newValue}
     * @param internalOakMap
     * @return {@code TRUE} if the value was written off-heap successfully
     * {@code FALSE} if the value is deleted (cannot be overwritten)
     * {@code RETRY} if the value was moved, if the chuck is frozen/released (prevents the moving of the value), or
     * if the version of the value does not match the version written inside {@code ctx}.
     * Along side the flag of the result, in case the exchange succeeded, it also returns the value that
     * was written before the exchange.
     */
    <V> Result exchange(
        Chunk<?, V> chunk, ThreadContext ctx, V value, OakTransformer<V> valueDeserializeTransformer,
        OakSerializer<V> serializer, InternalOakMap internalOakMap) {

        ValueResult result = ctx.value.s.lockWrite();
        if (result != ValueResult.TRUE) {
            return ctx.result.withFlag(result);
        }
        V oldValue = null;
        if (valueDeserializeTransformer != null) {
            oldValue = valueDeserializeTransformer.apply(ctx.value);
        }
        result = innerPut(chunk, ctx, value, serializer, internalOakMap);
        // in case move happened: ctx.value might be set to a new slice.
        // Alternatively, if returned result is RETRY, a rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        ctx.value.s.unlockWrite();
        return result == ValueResult.TRUE ? ctx.result.withValue(oldValue) : ctx.result.withFlag(ValueResult.RETRY);
    }

    /**
     * @param expected       the old value to which we compare the current value
     * @param internalOakMap
     * @return {@code TRUE} if the exchange went successfully
     * {@code FAILURE} if the value is deleted or if the actual value referenced in {@code ctx} does not equal to
     * {@code expected}
     * {@code RETRY} for the same reasons as exchange
     * @see #exchange(Chunk, ThreadContext, Object, OakTransformer, OakSerializer, InternalOakMap)
     */
    <V> ValueResult compareExchange(
        Chunk<?, V> chunk, ThreadContext ctx, V expected, V value,
        OakTransformer<V> valueDeserializeTransformer, OakSerializer<V> serializer,
        InternalOakMap internalOakMap) {

        ValueResult result = ctx.value.s.lockWrite();
        if (result != ValueResult.TRUE) {
            return result;
        }
        V oldValue = valueDeserializeTransformer.apply(ctx.value);
        if (!oldValue.equals(expected)) {
            ctx.value.s.unlockWrite();
            return ValueResult.FALSE;
        }
        result = innerPut(chunk, ctx, value, serializer, internalOakMap);
        // in case move happened: ctx.value might be set to a new allocation.
        // Alternatively, if returned result is RETRY, a rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        ctx.value.s.unlockWrite();
        return result;
    }
}
