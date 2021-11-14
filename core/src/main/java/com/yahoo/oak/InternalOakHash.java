/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.function.Consumer;

class InternalOakHash<K, V> extends InternalOakBasics<K, V> {
    /*-------------- Members --------------*/
    private final FirstLevelHashArray<K, V> hashArray;    // first level of indexing
    private final OakComparator<K> comparator;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    private final ValueUtils valueOperator;

    private static final int DEFAULT_MOST_SIGN_BITS_NUM = 16;
    static final int USE_DEFAULT_FIRST_TO_SECOND_BITS_PARTITION = -1;

    /*-------------- Constructors --------------*/
    InternalOakHash(OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
        OakComparator<K> oakComparator, MemoryManager vMM, MemoryManager kMM,
        ValueUtils valueOperator, int firstLevelBitSize, int secondLevelBitSize) {

        super(vMM, kMM);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.comparator = oakComparator;
        this.valueOperator = valueOperator;

        int msbForFirstLevelHash =
            (firstLevelBitSize == USE_DEFAULT_FIRST_TO_SECOND_BITS_PARTITION)
                ? DEFAULT_MOST_SIGN_BITS_NUM : firstLevelBitSize;

        this.hashArray =
            new FirstLevelHashArray<K, V>(msbForFirstLevelHash, secondLevelBitSize,
                this.size, vMM, kMM, oakComparator,
                keySerializer, valueSerializer, 1);

    }

    /**
     * Brings the OakHash to its initial state without entries
     * Used when we want to empty the structure without reallocating all the objects/memory
     * Exists only for hash, as for the map there are min keys in the off-heap memory
     * and the full clear method is more subtle
     * NOT THREAD SAFE !!!
     */
    void clear() {
        hashArray.clear();
        size.set(0);
        if (valuesMemoryManager != keysMemoryManager) {
            // Two memory managers are not the same instance, but they
            // may still have the same allocator
            if (valuesMemoryManager.getBlockMemoryAllocator()
                != keysMemoryManager.getBlockMemoryAllocator()) {
                // keys and values memory managers are not the same, and use different memory allocator
                valuesMemoryManager.clear(true);
                keysMemoryManager.clear(true);
                return;
            }
            // keys and values memory managers are not the same, but are pointing to the same memory allocator
            valuesMemoryManager.clear(true);
            keysMemoryManager.clear(false);
            return;
        }
        // keys and values memory managers are the same, it is enough to clear one
        valuesMemoryManager.clear(true);
    }

    /*-------------- Generic to specific rebalance --------------*/
    @Override
    protected void rebalanceBasic(BasicChunk<K, V> basicChunk) {
        rebalance((HashChunk<K, V>) basicChunk); // exception will be triggered on wrong type
    }

    /**
     * @param c - OrderedChunk to rebalance
     */
    private void rebalance(HashChunk<K, V> c) {
        // temporary throw an exception in order not to miss this erroneous behavior while debug
        throw new UnsupportedOperationException(
            "Not yet implemented rebalance of a HashChunk was requested");
        //TODO: to be implemented
    }

    /*----------- Centralized invocation of the default hash function on the input key -----------*/
    private int calculateKeyHash(K key, ThreadContext ctx) {
        if (ctx.operationKeyHash != EntryHashSet.INVALID_KEY_HASH) {
            return ctx.operationKeyHash;
        }

        // calculate by the hash function provided together with the serializer
        int keyHash = keySerializer.calculateHash(key);
        ctx.operationKeyHash = Math.abs(keyHash);
        return ctx.operationKeyHash;
    }

    /*-------------- Hash API Methods --------------*/
    // put the value associated with the key, if key existed old value is overwritten
    V put(K key, V value, OakTransformer<V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {

            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);
            // If there is a matching value reference for the given key, and it is not marked as deleted,
            // then this put changes the slice pointed by this value reference.
            if (ctx.isValueValid()) {
                // there is a value and it is not deleted
                Result res = valueOperator.exchange(c, ctx, value, transformer, valueSerializer);
                if (res.operationResult == ValueUtils.ValueResult.TRUE) {
                    return (V) res.value;
                }
                helpRebalanceIfInProgress(c);
                // Exchange failed because the value was deleted/moved between lookup and exchange. Continue with
                // insertion.
                continue;
            }

            if (!publishAndWriteKey(c, ctx, key, value)) {
                continue;
            }

            if (c.linkValue(ctx) != ValueUtils.ValueResult.TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return null; // null can be returned only in zero-copy case
            }
        }
        throw new RuntimeException("put failed: reached retry limit (1024).");
    }

    // returns false if restart and new search for the chunk is required
    private boolean publishAndWriteKey(HashChunk<K, V> c, ThreadContext ctx, K key, V value) {
        // for Hash case it is not necessary to help to finialize deletion of the found
        // (during lookup) entry as another entry may be chosen later (during allocation)
        if (isAfterRebalanceOrValueUpdate(c, ctx)) {
            return false;
        }

        // For OakHash publish the put earlier (compared to OakMap) in order to invoke delete
        // finalization (part of c.allocateEntryAndWriteKey) within publish/unpublish scope.
        // Delete finalization needs to be invoked within within publish/unpublish scope,
        // to ensure each slice is released only once
        if (!c.publish()) {
            rebalance(c);
            return false;
        }

        // AT THIS POINT EITHER (in all cases context is updated):
        // (1) Key wasn't found (key and value not valid)
        // (2) Key was found as part of deleted entry and deletion was accomplished if needed
        if (!c.allocateEntryAndWriteKey(ctx, key)) {
            // allocation wasn't successfull and resulted in rebalance - retry
            // currently cannot happen as rebalance is never requested due to too much collisions
            c.unpublish();
            rebalance(c);
            return false;
        }

        if (ctx.entryState == EntryArray.EntryState.VALID) {
            // the requested key already exists and can not be added
            // entry wasn't allocated and key wasn't written
            // returning false so the key will be caught on next look up
            c.unpublish();
            return false;
        }

        c.allocateValue(ctx, value, false); // write value in place
        return true;
    }

    /**
     * Used when value of a key was possibly moved and we try to search for the given key
     * through the OakMap again.
     *
     * @param ctx The context key should be initialized with the key to refresh, and the context value
     *            will be updated with the refreshed value.
     * @reutrn true if the refresh was successful.
     */
    @Override
    boolean refreshValuePosition(ThreadContext ctx) {
        K deserializedKey = keySerializer.deserialize(ctx.key);
        // find chunk matching key, puts this key hash into ctx.operationKeyHash
        HashChunk<K, V> c = hashArray.findChunk(
            deserializedKey, ctx, calculateKeyHash(deserializedKey, ctx));
        c.lookUp(ctx, deserializedKey);
        return ctx.isValueValid();
    }

    // if key exists, remove the key-value mapping from the map
    Result remove(K key, V oldValue, OakTransformer<V> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        // when logicallyDeleted is true, it means we have marked the value as deleted.
        // Note that the entry will remain linked until rebalance happens.
        boolean logicallyDeleted = false;
        V v = null;

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);

            if (!ctx.isKeyValid()) {
                // There is no such key. If we did logical deletion and someone else did the physical deletion,
                // then the old value is saved in v. Otherwise v is (correctly) null
                return transformer == null ? ctx.result.withFlag(logicallyDeleted) : ctx.result.withValue(v);
            } else if (!ctx.isValueValid()) {
                // There is such a key, but the value is invalid,
                // either deleted (maybe only off-heap) or not yet allocated
                if (!finalizeDeletion(c, ctx)) {
                    // finalize deletion returns false, meaning no rebalance was requested
                    // and there was an attempt to finalize deletion
                    return transformer == null ? ctx.result.withFlag(logicallyDeleted) : ctx.result.withValue(v);
                }
                continue;
            }

            // AT THIS POINT Key was found (key and value not valid) and context is updated
            if (logicallyDeleted) {
                // This is the case where we logically deleted this entry (marked the value off-heap as deleted),
                // but someone helped and (marked the value reference as deleted) and reused the entry
                // before we marked the value reference as deleted. We have the previous value saved in v.
                return transformer == null ? ctx.result.withFlag(ValueUtils.ValueResult.TRUE) : ctx.result.withValue(v);
            } else {
                Result removeResult = valueOperator.remove(ctx, oldValue, transformer);
                if (removeResult.operationResult == ValueUtils.ValueResult.FALSE) {
                    // we didn't succeed to remove the value: it didn't contain oldValue, or was already marked
                    // as deleted by someone else)
                    return ctx.result.withFlag(ValueUtils.ValueResult.FALSE);
                } else if (removeResult.operationResult == ValueUtils.ValueResult.RETRY) {
                    continue;
                }
                // we have marked this value as deleted (successful remove)
                logicallyDeleted = true;
                v = (V) removeResult.value;
            }

            // AT THIS POINT value was marked deleted off-heap by this thread,
            // continue to set the entry's value reference as deleted
            assert ctx.entryIndex != EntryArray.INVALID_ENTRY_INDEX;
            assert ctx.isValueValid();
            ctx.entryState = EntryArray.EntryState.DELETED_NOT_FINALIZED;

            if (inTheMiddleOfRebalance(c)) {
                continue;
            }

            // If finalize deletion returns true, meaning rebalance was done and there was NO
            // attempt to finalize deletion. There is going the help anyway, by next rebalance
            // or updater. Thus it is OK not to restart, the linearization point of logical deletion
            // is owned by this thread anyway and old value is kept in v.
            finalizeDeletion(c, ctx); // includes publish/unpublish
            return transformer == null ?
                ctx.result.withFlag(ValueUtils.ValueResult.TRUE) : ctx.result.withValue(v);
        }

        throw new RuntimeException("remove failed: reached retry limit (1024).");
    }

    private ThreadContext keyLookUp(K key) {

        if (key == null) {
            throw new NullPointerException();
        }
        ThreadContext ctx = getThreadContext();
        // find chunk matching key, puts this key hash into ctx.operationKeyHash
        HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
        c.lookUpForGetOnly(ctx, key);
        if (!ctx.isValueValid()) {
            return null;
        }
        return ctx;
    }

    // the zero-copy version of get
    OakUnscopedBuffer get(K key) {
        ThreadContext ctx = keyLookUp(key);
        if (ctx == null) {
            return null;
        }
        return getValueUnscopedBuffer(ctx);
    }

    <T> T getKeyTransformation(K key, OakTransformer<T> transformer) {
        ThreadContext ctx = keyLookUp(key);
        if (ctx == null) {
            return null;
        }
        return transformer.apply(ctx.key);
    }

    // the non-ZC variation of the get
    <T> T getValueTransformation(K key, OakTransformer<T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUpForGetOnly(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }

            Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
            if (res.operationResult == ValueUtils.ValueResult.RETRY) {
                continue;
            }
            return (T) res.value;
        }

        throw new RuntimeException("getValueTransformation failed: reached retry limit (1024).");
    }

    V replace(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }

            // will return null if the value is deleted
            Result result = valueOperator.exchange(c, ctx, value, valueDeserializeTransformer, valueSerializer);
            if (result.operationResult != ValueUtils.ValueResult.RETRY) {
                return (V) result.value;
            }
            // it might be that this chunk is proceeding with rebalance -> help
            helpRebalanceIfInProgress(c);
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    boolean replace(K key, V oldValue, V newValue, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return false;
            }

            ValueUtils.ValueResult res = valueOperator.compareExchange(c, ctx, oldValue, newValue,
                valueDeserializeTransformer, valueSerializer);
            if (res == ValueUtils.ValueResult.RETRY) {
                // it might be that this chunk is proceeding with rebalance -> help
                helpRebalanceIfInProgress(c);
                continue;
            }
            return res == ValueUtils.ValueResult.TRUE;
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    // put the value assosiated with the key, only if key didn't exist
    // returned results describes whether the value was inserted or not
    Result putIfAbsent(K key, V value, OakTransformer<V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);

            // If exists a matching value reference for the given key, and it isn't marked deleted,
            // organize the return value: false for ZC, and old value deserialization for non-ZC
            if (ctx.isValueValid()) {
                if (transformer == null) {
                    return ctx.result.withFlag(ValueUtils.ValueResult.FALSE);
                }
                Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
                if (res.operationResult == ValueUtils.ValueResult.TRUE) {
                    return res;
                }
                continue;
            }

            // TODO: For current version of OakHash we make an assumption that same keys aren't
            // TODO: updated simultaneously also not via putIfAbsent API. Therefore, if key wasn't
            // TODO: found till here, it is OK to proceed with normal put. But this needs to be
            // TODO: changed once this assignment is refined.

            if (!publishAndWriteKey(c, ctx, key, value)) {
                continue;
            }

            if (c.linkValue(ctx) != ValueUtils.ValueResult.TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return ctx.result.withFlag(ValueUtils.ValueResult.TRUE);
            }
        }

        throw new RuntimeException("putIfAbsent failed: reached retry limit (1024).");
    }

    // if key with a valid value exists in the map, apply compute function on the value
    // return true if compute did happen
    boolean computeIfPresent(K key, Consumer<OakScopedWriteBuffer> computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);

            if (ctx.isValueValid()) {
                ValueUtils.ValueResult res = valueOperator.compute(ctx.value, computer);
                if (res == ValueUtils.ValueResult.TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already marked as deleted, continue to construct another slice
                    return true;
                } else if (res == ValueUtils.ValueResult.RETRY) {
                    continue;
                }
            }
            return false;
        }

        throw new RuntimeException("computeIfPresent failed: reached retry limit (1024).");
    }

    // if key didn't exist, put the value to be associated with the key
    // otherwise perform compute on the existing value
    // return false if compute happened, true if put happened
    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakScopedWriteBuffer> computer) {
        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {

            if (i > MAX_RETRIES - 3) {
                System.err.println("Infinite loop..."); //TODO: remove this print
            }

            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(key, ctx, calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);

            // If there is a matching value reference for the given key, and it is not marked as deleted,
            // then apply compute on the existing value
            if (ctx.isValueValid()) {
                ValueUtils.ValueResult res = valueOperator.compute(ctx.value, computer);
                if (res == ValueUtils.ValueResult.TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already found as deleted, continue to allocate a new value slice
                    return false;
                } else if (res == ValueUtils.ValueResult.RETRY) {
                    continue;
                }
            }

            // TODO: For current version of OakHash we make an assumption that same keys aren't
            // TODO: updated simultaneously also not via putIfAbsentComputeIfPresent API.
            // TODO: Therefore, if key wasn't found till here, it is OK to proceed with normal put.
            // TODO: But this needs to be changed once this assignment is refined.

            if (!publishAndWriteKey(c, ctx, key, value)) {
                continue;
            }

            if (c.linkValue(ctx) != ValueUtils.ValueResult.TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return true;
            }
        }

        throw new RuntimeException("putIfAbsentComputeIfPresent failed: reached retry limit (1024).");
    }

    void printSummaryDebug() {
        hashArray.printSummaryDebug();
    }
}



