/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.function.Consumer;

class ValueUtilsImpl implements ValueUtils {

    /*-----------------------------------------------------------------------*/
    @Override
    public <T> Result transform(Result result, ValueBuffer value, OakTransformer<T> transformer) {
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

    @Override
    public <V> ValueResult put(Chunk<?, V> chunk, ThreadContext ctx, V newVal, OakSerializer<V> serializer,
                               MemoryManager memoryManager, InternalOakMap internalOakMap) {
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

    @Override
    public ValueResult compute(ValueBuffer value, Consumer<OakScopedWriteBuffer> computer) {
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

    @Override
    public <V> Result remove(ThreadContext ctx, V oldValue, OakTransformer<V> transformer) {
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

    @Override
    public <V> Result exchange(Chunk<?, V> chunk, ThreadContext ctx, V value,
        OakTransformer<V> valueDeserializeTransformer, OakSerializer<V> serializer,
        InternalOakMap internalOakMap) {
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

    @Override
    public <V> ValueResult compareExchange(Chunk<?, V> chunk, ThreadContext ctx, V expected, V value,
        OakTransformer<V> valueDeserializeTransformer, OakSerializer<V> serializer, InternalOakMap internalOakMap) {
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
