/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.function.Consumer;

interface ValueUtils {

    int VERSION_SIZE = 4;

    enum ValueResult {
        TRUE, FALSE, RETRY
    }

    /**
     * Some implementations of values which reside in the off-heap may have a header to it (e.g., the implementation of
     * Nova Values has a header of 8 bytes).
     */
    int getHeaderSize();

    /**
     * Some implementations of values which reside may have a lock in their header.
     */
    int getLockLocation();

    int getLockSize();

    /**
     * Acquires a read lock
     *
     * @param s the value's off-heap Slice object
     * @return {@code TRUE} if the read lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult lockRead(Slice s);

    /**
     * Releases a read lock
     *
     * @param s the value's off-heap Slice object
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult unlockRead(Slice s);

    /**
     * Acquires a write lock
     *
     * @param s the value's off-heap Slice object
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult lockWrite(Slice s);

    /**
     * Releases a write lock
     * Since a write lock is exclusive (unlike read lock), there is no way for the version to change, so no need to
     * pass it.
     *
     * @param s the value's off-heap Slice object
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult unlockWrite(Slice s);

    /**
     * Marks the value pointed by {@code s} as deleted only if the version of that value matches {@code version}.
     *
     * @param s the value's off-heap Slice object
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult deleteValue(Slice s);

    /**
     * @param s the value's off-heap Slice object
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult isValueDeleted(Slice s);

    /**
     * @param s the value's off-heap Slice object
     * @return the version of the value pointed by {@code s}
     */
    int getOffHeapVersion(Slice s);

    /**
     * Initializing the header version.
     * May also set other members in the header to their default values.
     *
     * @param s the value's off-heap Slice object
     */
    void initHeader(Slice s);

    /**
     * Initializing the header version and lock to be locked.
     * May also set other members in the header to their default values.
     *
     * @param s the value's off-heap Slice object
     */
    void initLockedHeader(Slice s);

    /* ==================== More complex methods on off-heap values ==================== */

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
    <T> Result transform(Result result, ValueBuffer value, OakTransformer<T> transformer);

    /**
     * @see #exchange(Chunk, ThreadContext, Object, OakTransformer, OakSerializer, MemoryManager, InternalOakMap)
     * Does not return the value previously written off-heap
     */
    <V> ValueResult put(Chunk<?, V> chunk, ThreadContext ctx, V newVal, OakSerializer<V> serializer,
                        MemoryManager memoryManager, InternalOakMap internalOakMap);

    /**
     * @param value    the value's off-heap Slice object
     * @param computer the function to apply on the Slice
     * @return {@code TRUE} if the function was applied successfully,
     * {@code FAILURE} if the value is deleted,
     * {@code RETRY} if the value was moved.
     */
    ValueResult compute(ValueBuffer value, Consumer<OakScopedWriteBuffer> computer);

    /**
     * Marks a value as deleted and frees its slice (whether the header is freed or not is implementation dependant).
     *
     * @param ctx           has the entry index and its value to be remove
     * @param memoryManager the memory manager to which the slice is returned to
     * @param oldValue      in case of a conditional remove, this is the value to which the actual value is compared to
     * @param transformer   value deserializer
     * @param <V>           the type of the value
     * @return {@code TRUE} if the value was removed successfully,
     * {@code FALSE} if the value is already deleted, or if it does not equal to {@code oldValue}
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     * In case of success, the value of the returned Result is the value which was written in the off-heap before the
     * removal (if {@code transformer} is not null), otherwise, it is {@code null}.
     */
    <V> Result remove(ThreadContext ctx, MemoryManager memoryManager, V oldValue,
                      OakTransformer<V> transformer);

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
     * @param memoryManager               the memory manager to free a slice with is not needed after the value moved
     * @param internalOakMap
     * @return {@code TRUE} if the value was written off-heap successfully
     * {@code FALSE} if the value is deleted (cannot be overwritten)
     * {@code RETRY} if the value was moved, if the chuck is frozen/released (prevents the moving of the value), or
     * if the version of the value does not match the version written inside {@code ctx}.
     * Along side the flag of the result, in case the exchange succeeded, it also returns the value that
     * was written before the exchange.
     */
    <V> Result exchange(Chunk<?, V> chunk, ThreadContext ctx, V value, OakTransformer<V> valueDeserializeTransformer,
                        OakSerializer<V> serializer, MemoryManager memoryManager, InternalOakMap internalOakMap);

    /**
     * @param expected       the old value to which we compare the current value
     * @param internalOakMap
     * @return {@code TRUE} if the exchange went successfully
     * {@code FAILURE} if the value is deleted or if the actual value referenced in {@code ctx} does not equal to
     * {@code expected}
     * {@code RETRY} for the same reasons as exchange
     * @see #exchange(Chunk, ThreadContext, Object, OakTransformer, OakSerializer, MemoryManager, InternalOakMap)
     */
    <V> ValueResult compareExchange(Chunk<?, V> chunk, ThreadContext ctx, V expected, V value,
                                    OakTransformer<V> valueDeserializeTransformer, OakSerializer<V> serializer,
                                    MemoryManager memoryManager, InternalOakMap internalOakMap);
}
