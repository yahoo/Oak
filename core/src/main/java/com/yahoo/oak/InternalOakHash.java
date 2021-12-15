/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

class InternalOakHash<K, V> extends InternalOakBasics<K, V> {
    /*-------------- Members --------------*/
    private final FirstLevelHashArray<K, V> hashArray;    // first level of indexing

    private final ValueUtils valueOperator;

    private static final int DEFAULT_MOST_SIGN_BITS_NUM = 16;
    static final int USE_DEFAULT_FIRST_TO_SECOND_BITS_PARTITION = -1;

    /*-------------- Constructors --------------*/
    InternalOakHash(OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
        OakComparator<K> oakComparator, MemoryManager vMM, MemoryManager kMM,
        ValueUtils valueOperator, int firstLevelBitSize, int secondLevelBitSize) {

        super(vMM, kMM, keySerializer, valueSerializer);

        this.valueOperator = valueOperator;

        int msbForFirstLevelHash =
            (firstLevelBitSize == USE_DEFAULT_FIRST_TO_SECOND_BITS_PARTITION)
                ? DEFAULT_MOST_SIGN_BITS_NUM : firstLevelBitSize;

        this.hashArray =
            new FirstLevelHashArray<>(msbForFirstLevelHash, secondLevelBitSize,
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
        if (getValuesMemoryManager() != getKeysMemoryManager()) {
            // Two memory managers are not the same instance, but they
            // may still have the same allocator
            if (getValuesMemoryManager().getBlockMemoryAllocator()
                != getKeysMemoryManager().getBlockMemoryAllocator()) {
                // keys and values memory managers are not the same, and use different memory allocator
                getValuesMemoryManager().clear(true);
                getKeysMemoryManager().clear(true);
                return;
            }
            // keys and values memory managers are not the same, but are pointing to the same memory allocator
            getValuesMemoryManager().clear(true);
            getKeysMemoryManager().clear(false);
            return;
        }
        // keys and values memory managers are the same, it is enough to clear one
        getValuesMemoryManager().clear(true);
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
        int keyHash = getKeySerializer().calculateHash(key);
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
            HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);
            // If there is a matching value reference for the given key, and it is not marked as deleted,
            // then this put changes the slice pointed by this value reference.
            if (ctx.isValueValid()) {
                // there is a value and it is not deleted
                Result res = valueOperator.exchange(c, ctx, value, transformer, getValueSerializer());
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
     * @return true if the refresh was successful.
     */
    @Override
    boolean refreshValuePosition(ThreadContext ctx) {
        K deserializedKey = getKeySerializer().deserialize(ctx.key);
        // find chunk matching key, puts this key hash into ctx.operationKeyHash
        HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(deserializedKey, ctx));
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
            HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
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
        HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
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


    //private <T> T getValueTransformation(OakScopedReadBuffer key, OakTransformer<T> transformer) {
    //    K deserializedKey = getKeySerializer().deserialize(key);
    //    return getValueTransformation(deserializedKey, transformer);
    //}

    // the non-ZC variation of the get
    <T> T getValueTransformation(K key, OakTransformer<T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
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
            HashChunk<K, V> chunk = hashArray.findChunk(calculateKeyHash(key, ctx));
            chunk.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }

            // will return null if the value is deleted
            Result result = valueOperator.exchange(chunk, ctx, value,
                    valueDeserializeTransformer, getValueSerializer());
            if (result.operationResult != ValueUtils.ValueResult.RETRY) {
                return (V) result.value;
            }
            // it might be that this chunk is proceeding with rebalance -> help
            helpRebalanceIfInProgress(chunk);
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    boolean replace(K key, V oldValue, V newValue, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            // find chunk matching key, puts this key hash into ctx.operationKeyHash
            HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return false;
            }

            ValueUtils.ValueResult res = valueOperator.compareExchange(c, ctx, oldValue, newValue,
                valueDeserializeTransformer, getValueSerializer());
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
            HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
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
            HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
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
            HashChunk<K, V> c = hashArray.findChunk(calculateKeyHash(key, ctx));
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

    //////////////////////////////////////////////////////////////////////////////
    /*-------------- Iterators --------------*/
    //////////////////////////////////////////////////////////////////////////////

    private static final class IteratorState<K, V> extends BasicIteratorState<K, V> {

        private KeyBuffer lastKeyAccessed = null;
        private boolean keyBufferValid = false;

        private IteratorState(HashChunk<K, V> nextHashChunk, HashChunk<K, V>.HashChunkIter nextChunkIter,
                              int nextIndex) {
            super(nextHashChunk, nextChunkIter, nextIndex);
        }

        private KeyBuffer getKeyBuffer() {
            if (keyBufferValid) {
                return lastKeyAccessed;
            } else {
                return null;
            }
        }

        private void setKeyBuffer(KeyBuffer newBuffer) {
            lastKeyAccessed = newBuffer;
        }

        private void setKeyValid(boolean keyBufferValid) {
            this.keyBufferValid = keyBufferValid;
        }

        static <K, V> InternalOakHash.IteratorState<K, V> newInstance(
                HashChunk<K, V> nextHashChunk, HashChunk<K, V>.HashChunkIter nextChunkIter) {

            return new InternalOakHash.IteratorState<>(nextHashChunk, nextChunkIter, HashChunk.INVALID_ENTRY_INDEX);
        }

    }

    /**
     * Base of iterator classes:
     */
    abstract class HashIter<T> extends BasicIter<T> {

        HashIter() {
            initState();
        }
        @Override
        protected void initAfterRebalance() {
            //TODO - refactor to use OakReadBuffer without deserializing.
            getState().getChunk().readKeyFromEntryIndex(ctx.tempKey, getState().getIndex());
            K nextKey = getKeySerializer().deserialize(ctx.tempKey);


            // Update the state to point to last returned key.
            initState();
        }


        /**
         * Advances next to higher entry.
         *  previous index
         *
         * The first long is the key's reference, the integer is the value's version and the second long is
         * the value's reference. If {@code needsValue == false}, then the value of the map entry is {@code null}.
         */
        @Override
        void advance(boolean needsValue) {
            boolean validState = false;

            while (!validState) {
                if (getState() == null) {
                    throw new NoSuchElementException();
                }

                final BasicChunk<K, V> chunk = getState().getChunk();
                if (chunk.state() == BasicChunk.State.RELEASED) {
                    throw new UnsupportedOperationException("rebalance in not supported by InternalOakHash iterator");
                }

                final int curIndex = getState().getIndex();

                // build the entry context that sets key references and does not check for value validity.
                ctx.initEntryContext(curIndex);


                chunk.readKey(ctx);

                ((IteratorState) getState()).setKeyBuffer(ctx.key);
                ((IteratorState) getState()).setKeyValid(ctx.isKeyValid());
                validState = ctx.isKeyValid();

                if (validState & needsValue) {
                    // Set value references and checks for value validity.
                    // if value is deleted ctx.entryState is going to be invalid

                    chunk.readValue(ctx);
                    validState = ctx.isValueValid();
                }

                advanceState();
            }
        }

        /**
         * Advances next to the next entry without creating a ByteBuffer for the key.
         * Return previous index
         */
        @Override
        protected void  advanceStream(UnscopedBuffer<KeyBuffer> key, UnscopedBuffer<ValueBuffer> value) {
            assert key != null || value != null;

            boolean validState = false;

            while (!validState) {
                if (getState() == null) {
                    throw new NoSuchElementException();
                }

                final BasicChunk<K, V> c = getState().getChunk();
                if (c.state() == BasicChunk.State.RELEASED) {
                    initAfterRebalance();
                    continue;
                }

                final int curIndex = getState().getIndex();

                if (key != null) {
                    validState = c.readKeyFromEntryIndex(key.getInternalScopedReadBuffer(), curIndex);
                    ((IteratorState) getState()).setKeyBuffer(key.getInternalScopedReadBuffer());
                    ((IteratorState) getState()).setKeyValid(validState);

                    assert validState;
                }

                if (value != null) {
                    // If the current value is deleted, then advance and try again
                    validState = c.readValueFromEntryIndex(value.getInternalScopedReadBuffer(), curIndex);
                }

                advanceState();
            }
        }

        protected void initState() {

            HashChunk<K, V>.HashChunkIter nextChunkIter;
            HashChunk<K, V> nextHashChunk;
            int fstChunkIdx = 0;

            nextHashChunk = hashArray.getChunk(fstChunkIdx);
            if (nextHashChunk != null) {
                nextChunkIter = nextHashChunk.chunkIter(ctx);
            } else {
                setState(null);
                return;
            }


            //Init state, not valid yet, must move forward
            setState(InternalOakHash.IteratorState.newInstance(nextHashChunk, nextChunkIter));
            ((IteratorState) getState()).setKeyValid(false);
            advanceState();
        }

        @Override
        protected BasicChunk<K, V> getNextChunk(BasicChunk<K, V> current) {
            KeyBuffer keyBuffer = ((IteratorState) getState()).getKeyBuffer();
            int lastKeyHash = 0;
            boolean hashValid;
            if (keyBuffer == null) {
                hashValid = false;
            } else {
                K deserializedKey = getKeySerializer().deserialize(ctx.key);

                lastKeyHash = calculateKeyHash(deserializedKey, ctx);
                hashValid = true;
            }
            return hashArray.getNextChunk(current, lastKeyHash, hashValid);
        }

        @Override
        protected void advanceState() {

            HashChunk<K, V> hashChunk = (HashChunk<K, V>) getState().getChunk();
            BasicChunk.BasicChunkIter chunkIter = getState().getChunkIter();


            while (!chunkIter.hasNext()) { // chunks can have only removed keys
                hashChunk = (HashChunk<K, V>) getNextChunk(hashChunk);
                if (hashChunk == null) {
                    //End of iteration
                    setState(null);
                    return;
                }
                chunkIter = getChunkIter(hashChunk);
            }

            int nextIndex = chunkIter.next(ctx);
            getState().set(hashChunk, chunkIter, nextIndex);
        }

        protected HashChunk<K, V>.HashChunkIter getChunkIter(HashChunk<K, V> current) {
            return current.chunkIter(ctx);
        }
    }

    class ValueIterator extends HashIter<OakUnscopedBuffer> {

        @Override
        public OakUnscopedBuffer next() {
            advance(true);
            return getValueUnscopedBuffer(ctx);
        }
    }

    class ValueStreamIterator extends HashIter<OakUnscopedBuffer> {

        private final UnscopedBuffer<ValueBuffer> value =
                new UnscopedBuffer<>(new ValueBuffer(getValuesMemoryManager().getEmptySlice()));

        @Override
        public OakUnscopedBuffer next() {
            advanceStream(null, value);
            return value;
        }
    }

    class ValueTransformIterator<T> extends HashIter<T> {

        final OakTransformer<T> transformer;

        ValueTransformIterator(OakTransformer<T> transformer) {
            this.transformer = transformer;
        }

        public T next() {
            advance(true);
            Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
            // If this value is deleted, try the next one
            if (res.operationResult == ValueUtils.ValueResult.FALSE) {
                return next();
            } else if (res.operationResult == ValueUtils.ValueResult.RETRY) {
                // if the value was moved, fetch it from the
                T result = getValueTransformation(ctx.key, transformer);
                if (result == null) {
                    // the value was deleted, try the next one
                    return next();
                }
                return result;
            }
            return (T) res.value;
        }
    }

    class EntryIterator extends HashIter<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> {

        public Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> next() {
            advance(true);
            return new AbstractMap.SimpleImmutableEntry<>(getKeyUnscopedBuffer(ctx), getValueUnscopedBuffer(ctx));
        }
    }

    class EntryStreamIterator extends HashIter<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>>
            implements Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> {

        private final UnscopedBuffer<KeyBuffer> key =
                new UnscopedBuffer<>(new KeyBuffer(getKeysMemoryManager().getEmptySlice()));
        private final UnscopedBuffer<ValueBuffer> value =
                new UnscopedBuffer<>(new ValueBuffer(getValuesMemoryManager().getEmptySlice()));

        EntryStreamIterator() {
            super();
        }

        public Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> next() {
            advanceStream(key, value);
            return this;
        }

        @Override
        public OakUnscopedBuffer getKey() {
            return key;
        }

        @Override
        public OakUnscopedBuffer getValue() {
            return value;
        }

        @Override
        public OakUnscopedBuffer setValue(OakUnscopedBuffer value) {
            throw new UnsupportedOperationException();
        }
    }

    class EntryTransformIterator<T> extends HashIter<T> {

        final Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>, T> transformer;

        EntryTransformIterator(Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>, T> transformer) {
            assert (transformer != null);
            this.transformer = transformer;
        }

        public T next() {
            advance(true);
            ValueUtils.ValueResult res = ctx.value.s.lockRead();
            if (res == ValueUtils.ValueResult.FALSE) {
                return next();
            } else if (res == ValueUtils.ValueResult.RETRY) {
                do {
                    boolean isSuccessful = refreshValuePosition(ctx);
                    if (!isSuccessful) {
                        return next();
                    }
                    res = ctx.value.s.lockRead();
                } while (res != ValueUtils.ValueResult.TRUE);
            }

            Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer> entry =
                    new AbstractMap.SimpleEntry<>(ctx.key, ctx.value);

            T transformation = transformer.apply(entry);
            ctx.value.s.unlockRead();
            return transformation;
        }
    }

    // May return deleted keys
    class KeyIterator extends HashIter<OakUnscopedBuffer> {

        @Override
        public OakUnscopedBuffer next() {
            advance(false);
            return getKeyUnscopedBuffer(ctx);

        }
    }

    public class KeyStreamIterator extends HashIter<OakUnscopedBuffer> {

        private final UnscopedBuffer<KeyBuffer> key
                = new UnscopedBuffer<>(new KeyBuffer(getKeysMemoryManager().getEmptySlice()));

        @Override
        public OakUnscopedBuffer next() {
            advanceStream(key, null);
            return key;
        }
    }

    class KeyTransformIterator<T> extends HashIter<T> {

        final OakTransformer<T> transformer;

        KeyTransformIterator(OakTransformer<T> transformer) {
            this.transformer = transformer;
        }

        public T next() {
            advance(false);
            return transformer.apply(ctx.key);
        }
    }

    // Factory methods for iterators

    Iterator<OakUnscopedBuffer> valuesBufferViewIterator() {
        return new ValueIterator();
    }

    Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesBufferViewIterator() {
        return new EntryIterator();
    }

    Iterator<OakUnscopedBuffer> keysBufferViewIterator() {
        return new KeyIterator();
    }

    Iterator<OakUnscopedBuffer> valuesStreamIterator() {
        return new ValueStreamIterator();
    }

    Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesStreamIterator() {
        return new EntryStreamIterator();
    }

    Iterator<OakUnscopedBuffer> keysStreamIterator() {
        return new KeyStreamIterator();
    }

    <T> Iterator<T> valuesTransformIterator(OakTransformer<T> transformer) {
        return new ValueTransformIterator<>(transformer);
    }

    <T> Iterator<T> entriesTransformIterator(Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>,
                                                     T> transformer) {
        return new EntryTransformIterator<>(transformer);
    }

    <T> Iterator<T> keysTransformIterator(OakTransformer<T> transformer) {
        return new KeyTransformIterator<>(transformer);
    }

}



