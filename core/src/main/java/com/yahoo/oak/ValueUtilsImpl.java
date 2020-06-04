/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.util.function.Consumer;

class ValueUtilsImpl implements ValueUtils {
    enum LockStates {
        FREE(0), LOCKED(1), DELETED(2), MOVED(3);

        public final int value;

        LockStates(int value) {
            this.value = value;
        }
    }

    private static final int LOCK_MASK = 0x3;
    private static final int LOCK_SHIFT = 2;
    private static final int VALUE_HEADER_SIZE = 4;

    private static Unsafe unsafe = UnsafeUtils.unsafe;

    private static boolean cas(Slice s, int expectedLock, int newLock, int version) {
        long address = s.getMetadataAddress();

        // Since the writing is done directly to the memory, the endianness of the memory is important here.
        // Therefore, we make sure that the values are read and written correctly.
        long expected = UnsafeUtils.intsToLong(version, expectedLock);
        long value = UnsafeUtils.intsToLong(version, newLock);
        return unsafe.compareAndSwapLong(null, address, expected, value);
    }

    @Override
    public <T> Result transform(Result result, ValueBuffer value, OakTransformer<T> transformer) {
        ValueResult ret = lockRead(value);
        if (ret != ValueResult.TRUE) {
            return result.withFlag(ret);
        }

        try {
            T transformation = transformer.apply(value);
            return result.withValue(transformation);
        } finally {
            unlockRead(value);
        }
    }

    @Override
    public <V> ValueResult put(Chunk<?, V> chunk, ThreadContext ctx, V newVal, OakSerializer<V> serializer,
                               MemoryManager memoryManager, InternalOakMap internalOakMap) {
        ValueResult result = lockWrite(ctx.value);
        if (result != ValueResult.TRUE) {
            return result;
        }
        result = innerPut(chunk, ctx, newVal, serializer, memoryManager, internalOakMap);
        // in case move happened: ctx.valueSlice might be set to a new slice.
        // Alternatively, if returned result is RETRY, a rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        unlockWrite(ctx.value);
        return result;
    }

    private <V> ValueResult innerPut(Chunk<?, V> chunk, ThreadContext ctx, V newVal, OakSerializer<V> serializer,
                                     MemoryManager memoryManager, InternalOakMap internalOakMap) {
        int capacity = serializer.calculateSize(newVal);
        if (capacity > ctx.value.getLength()) {
            return moveValue(chunk, ctx, memoryManager, internalOakMap, newVal);
        }
        ScopedWriteBuffer.serialize(ctx.value, newVal, serializer);
        return ValueResult.TRUE;
    }

    private <V> ValueResult moveValue(
            Chunk<?, V> chunk, ThreadContext ctx, MemoryManager memoryManager,
            InternalOakMap internalOakMap, V newVal) {

        boolean moved = internalOakMap.overwriteExistingValueForMove(ctx, newVal, chunk);
        if (!moved) {
            // rebalance was needed or the entry was updated by someone else, need to retry
            return ValueResult.RETRY;
        }
        // can not release the old slice or mark it moved, before the new one is updated!
        setLockState(ctx.value, LockStates.MOVED);
        // currently the slices which value was moved aren't going to be released, to keep the MOVED mark
        // TODO: deal with the reallocation of the moved memory

        ctx.value.copyFrom(ctx.newValue);
        return ValueResult.TRUE;
    }

    @Override
    public ValueResult compute(ValueBuffer value, Consumer<OakScopedWriteBuffer> computer) {
        ValueResult result = lockWrite(value);
        if (result != ValueResult.TRUE) {
            return result;
        }

        try {
            ScopedWriteBuffer.compute(value, computer);
        } finally {
            unlockWrite(value);
        }

        return ValueResult.TRUE;
    }

    @Override
    public <V> Result remove(ThreadContext ctx, MemoryManager memoryManager, V oldValue,
                             OakTransformer<V> transformer) {
        // Not a conditional remove, so we can delete immediately
        if (oldValue == null) {
            // try to delete
            ValueResult result = deleteValue(ctx.value);
            if (result != ValueResult.TRUE) {
                return ctx.result.withFlag(result);
            }
            // Now the value is deleted, and all other threads will treat it as deleted, but it is not yet freed, so
            // this thread can read from it.
            // read the old value (the slice is not reclaimed yet)
            V v = transformer != null ? transformer.apply(ctx.value) : null;
            // return TRUE with the old value
            return ctx.result.withValue(v);
        } else {
            // This is a conditional remove, so we first have to check whether the current value matches the expected
            // one.
            // We start by acquiring a write lock for reading since we do not want concurrent reads.
            ValueResult result = lockWrite(ctx.value);
            if (result != ValueResult.TRUE) {
                return ctx.result.withFlag(result);
            }
            V v = transformer.apply(ctx.value);
            // This is where we check the equality between the expected value and the actual value
            if (!oldValue.equals(v)) {
                unlockWrite(ctx.value);
                return ctx.result.withFlag(ValueResult.FALSE);
            }
            // both values match so the value is marked as deleted. No need for a CAS since a write lock is exclusive
            setLockState(ctx.value, LockStates.DELETED);
            // delete the value in the entry happens next and the slice will be released as part of it
            // slice can be released only after the entry is marked appropriately
            return ctx.result.withValue(v);
        }
    }

    @Override
    public <V> Result exchange(Chunk<?, V> chunk, ThreadContext ctx, V value,
                               OakTransformer<V> valueDeserializeTransformer, OakSerializer<V> serializer,
                               MemoryManager memoryManager, InternalOakMap internalOakMap) {
        ValueResult result = lockWrite(ctx.value);
        if (result != ValueResult.TRUE) {
            return ctx.result.withFlag(result);
        }
        V oldValue = null;
        if (valueDeserializeTransformer != null) {
            oldValue = valueDeserializeTransformer.apply(ctx.value);
        }
        result = innerPut(chunk, ctx, value, serializer, memoryManager, internalOakMap);
        // in case move happened: ctx.value might be set to a new slice.
        // Alternatively, if returned result is RETRY, a rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        unlockWrite(ctx.value);
        return result == ValueResult.TRUE ? ctx.result.withValue(oldValue) : ctx.result.withFlag(ValueResult.RETRY);
    }

    @Override
    public <V> ValueResult compareExchange(Chunk<?, V> chunk, ThreadContext ctx, V expected, V value,
                                           OakTransformer<V> valueDeserializeTransformer, OakSerializer<V> serializer,
                                           MemoryManager memoryManager, InternalOakMap internalOakMap) {
        ValueResult result = lockWrite(ctx.value);
        if (result != ValueResult.TRUE) {
            return result;
        }
        V oldValue = valueDeserializeTransformer.apply(ctx.value);
        if (!oldValue.equals(expected)) {
            unlockWrite(ctx.value);
            return ValueResult.FALSE;
        }
        result = innerPut(chunk, ctx, value, serializer, memoryManager, internalOakMap);
        // in case move happened: ctx.value might be set to a new allocation.
        // Alternatively, if returned result is RETRY, a rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        unlockWrite(ctx.value);
        return result;
    }

    @Override
    public int getHeaderSize() {
        return getLockSize() + getLockLocation();
    }

    @Override
    public int getLockLocation() {
        return VERSION_SIZE;
    }

    @Override
    public int getLockSize() {
        return VALUE_HEADER_SIZE;
    }

    @Override
    public ValueResult lockRead(Slice s) {
        int lockState;
        final int version = s.getVersion();
        assert version > EntrySet.INVALID_VERSION;
        do {
            int oldVersion = getOffHeapVersion(s);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            lockState = getLockState(s);
            if (oldVersion != getOffHeapVersion(s)) {
                return ValueResult.RETRY;
            }
            if (lockState == LockStates.DELETED.value) {
                return ValueResult.FALSE;
            }
            if (lockState == LockStates.MOVED.value) {
                return ValueResult.RETRY;
            }
            lockState &= ~LOCK_MASK;
        } while (!cas(s, lockState, lockState + (1 << LOCK_SHIFT), version));
        return ValueResult.TRUE;
    }

    @Override
    public ValueResult unlockRead(Slice s) {
        int lockState;
        final int version = s.getVersion();
        assert version > EntrySet.INVALID_VERSION;
        do {
            lockState = getLockState(s);
            assert lockState > LockStates.MOVED.value;
            lockState &= ~LOCK_MASK;
        } while (!cas(s, lockState, lockState - (1 << LOCK_SHIFT), version));
        return ValueResult.TRUE;
    }

    @Override
    public ValueResult lockWrite(Slice s) {
        final int version = s.getVersion();
        assert version > EntrySet.INVALID_VERSION;
        do {
            int oldVersion = getOffHeapVersion(s);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            int lockState = getLockState(s);
            if (oldVersion != getOffHeapVersion(s)) {
                return ValueResult.RETRY;
            }
            if (lockState == LockStates.DELETED.value) {
                return ValueResult.FALSE;
            }
            if (lockState == LockStates.MOVED.value) {
                return ValueResult.RETRY;
            }
        } while (!cas(s, LockStates.FREE.value, LockStates.LOCKED.value, version));
        return ValueResult.TRUE;
    }

    @Override
    public ValueResult unlockWrite(Slice s) {
        setLockState(s, LockStates.FREE);
        return ValueResult.TRUE;
    }

    @Override
    public ValueResult deleteValue(Slice s) {
        final int version = s.getVersion();
        assert version > EntrySet.INVALID_VERSION;
        do {
            int oldVersion = getOffHeapVersion(s);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            int lockState = getLockState(s);
            if (oldVersion != getOffHeapVersion(s)) {
                return ValueResult.RETRY;
            }
            if (lockState == LockStates.DELETED.value) {
                return ValueResult.FALSE;
            }
            if (lockState == LockStates.MOVED.value) {
                return ValueResult.RETRY;
            }
        } while (!cas(s, LockStates.FREE.value, LockStates.DELETED.value, version));
        return ValueResult.TRUE;
    }

    @Override
    public ValueResult isValueDeleted(Slice s) {
        final int version = s.getVersion();
        int oldVersion = getOffHeapVersion(s);
        if (oldVersion != version) {
            return ValueResult.RETRY;
        }
        int lockState = getLockState(s);
        if (oldVersion != getOffHeapVersion(s)) {
            return ValueResult.RETRY;
        }
        if (lockState == LockStates.MOVED.value) {
            return ValueResult.RETRY;
        }
        if (lockState == LockStates.DELETED.value) {
            return ValueResult.TRUE;
        }
        return ValueResult.FALSE;
    }

    @Override
    public int getOffHeapVersion(Slice s) {
        return getInt(s, 0);
    }

    private void setVersion(Slice s) {
        putInt(s, 0, s.getVersion());
    }

    private int getLockState(Slice s) {
        return getInt(s, getLockLocation());
    }

    private void setLockState(Slice s, LockStates state) {
        putInt(s, getLockLocation(), state.value);
    }

    @Override
    public void initHeader(Slice s) {
        initHeader(s, LockStates.FREE);
    }

    @Override
    public void initLockedHeader(Slice s) {
        initHeader(s, LockStates.LOCKED);
    }

    private void initHeader(Slice s, ValueUtilsImpl.LockStates state) {
        setVersion(s);
        setLockState(s, state);
    }

    private int getInt(Slice s, int index) {
        return unsafe.getInt(s.getMetadataAddress() + index);
    }

    private void putInt(Slice s, int index, int value) {
        unsafe.putInt(s.getMetadataAddress() + index, value);
    }
}
