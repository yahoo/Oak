/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicInteger;

class HashChunk<K, V> extends BasicChunk<K, V> {
    // defaults
    public static final int HASH_CHUNK_MAX_ITEMS_DEFAULT = 1024;

    // HashChunk takes a number of least significant bits from the full key hash
    // to provide as an index in the EntryHashSet
    private final UnionCodec hashIndexCodec; // to be given

    /*-------------- Members --------------*/
    private final EntryHashSet<K, V> entryHashSet;

    /*-------------- Constructors --------------*/
    /**
     * This constructor is only used when creating the first ever chunk/chunks (without a creator).
     * The caller might set the creator before returning the HashChunk to the user.
     *
     * @param maxItems  is the size of the entries array (not all the entries are going to be in use)
     *                  IMPORTANT!: it is better to be a power of two,
     *                  if not the rest of the entries are going to be waisted
     *
     * Why do we allow parameter maxItems, and not rely only on hashIndexCodec.getFirstBitSize()
     * in the power of 2? As general HashChunk can be bigger than 2^bitSize. For example, it can
     * be defined that the "calculated" keyHash (index in the entry array) will always be even, and
     * odd entries will be for the collisions only. There can be a reason to do bigger chunk and not
     * to enlarge the FirstLevelHashArray.
     *
     * @param hashIndexCodec the codec initiated with the right amount of the least significant bits,
     *                       to be used to get index from the key hash
     */
    HashChunk(int maxItems, AtomicInteger externalSize, MemoryManager vMM, MemoryManager kMM,
        OakComparator<K> comparator, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer, UnionCodec hashIndexCodec) {

        super(maxItems, externalSize, comparator);
        assert Math.pow( 2, hashIndexCodec.getFirstBitSize() ) <= maxItems ;

        this.hashIndexCodec = hashIndexCodec;
        this.entryHashSet = // must be called after setSecondLevelBitsThreshold
            new EntryHashSet<>(vMM, kMM, getMaxItems(), keySerializer, valueSerializer,
                comparator);
    }

    /**
     * Create a child HashChunk where this HashChunk object as its creator.
     */
    HashChunk<K, V> createChild(UnionCodec hashIndexCodec) {
        HashChunk<K, V> child =
            new HashChunk<>(getMaxItems(), externalSize,
                entryHashSet.valuesMemoryManager, entryHashSet.keysMemoryManager,
                comparator, entryHashSet.keySerializer, entryHashSet.valueSerializer,
                hashIndexCodec);
        updateBasicChild(child);
        return child;
    }

    private int calculateKeyHash(K key, ThreadContext ctx) {
        if (ctx.operationKeyHash == EntryHashSet.INVALID_KEY_HASH) {
            // should happen only during component unit test!
            ctx.operationKeyHash = Math.abs(key.hashCode());
        }
        // hash was already calculated by hash array when looking for the chunk and kept in thread context
        return ctx.operationKeyHash;
    }
    /********************************************************************************************/
    /*-----------------------------  Wrappers for EntryOrderedSet methods -----------------------------*/

    /**
     * See {@code EntryHashSet.isValueRefValidAndNotDeleted(int)} for more information
     */
    boolean isValueRefValidAndNotDeleted(int ei) {
        return entryHashSet.isValueRefValidAndNotDeleted(ei);
    }

    /**
     * See {@code EntryHashSet.readKey(ThreadContext)} for more information
     */
    void readKey(ThreadContext ctx) {
        entryHashSet.readKey(ctx);
    }

    /**
     * See {@code EntryHashSet.readValue(ThreadContext)} for more information
     */
    void readValue(ThreadContext ctx) {
        entryHashSet.readValue(ctx);
    }

    /**
     * See {@code EntryHashSet.readKey(KeyBuffer)} for more information
     */
    boolean readKeyFromEntryIndex(KeyBuffer key, int ei) {
        return entryHashSet.readKey(key, ei);
    }

    /**
     * See {@code EntryHashSet.readValue(ValueBuffer)} for more information
     */
    boolean readValueFromEntryIndex(ValueBuffer value, int ei) {
        return entryHashSet.readValue(value, ei);
    }

    /**
     * Writes the key off-heap and allocates an entry with the reference pointing to the given key
     * See {@code EntryHashSet.allocateEntryAndWriteKey(ThreadContext)} for more information
     * Returns false if rebalance is required and true otherwise
     */
    @Override
    boolean allocateEntryAndWriteKey(ThreadContext ctx, K key) {
        int keyHash = calculateKeyHash(key, ctx);
        return entryHashSet.allocateEntryAndWriteKey(
            ctx, key, calculateEntryIdx(key, keyHash), keyHash);
    }

    /**
     * See {@code EntryHashSet.allocateValue(ThreadContext)} for more information
     */
    @Override
    void allocateValue(ThreadContext ctx, V value, boolean writeForMove) {
        entryHashSet.allocateValue(ctx, value, writeForMove);
    }

    /**
     * See {@code EntryHashSet.releaseKey(ThreadContext)} for more information
     */
    @Override
    void releaseKey(ThreadContext ctx) {
        entryHashSet.releaseKey(ctx);
    }

    /**
     * See {@code EntryHashSet.releaseNewValue(ThreadContext)} for more information
     */
    @Override
    void releaseNewValue(ThreadContext ctx) {
        entryHashSet.releaseNewValue(ctx);
    }

    /**
     * Brings the chunk to its initial state without entries
     * Used when we want to empty the structure without reallocating all the objects/memory
     * Exists only for hash, as for the map there are min keys in the off-heap memory
     * and the full clear method is more subtle
     * NOT THREAD SAFE !!!
     */
    void clear() {
        entryHashSet.clear();
    }
    /********************************************************************************************/
    /*-----------------------  Methods for looking up item in this chunk -----------------------*/

    private int calculateEntryIdx(K key, int keyHash) {
        // hashIndexCodec in chunk (in different chunks) and in FirstLevelHashArray may differ
        // first and not second, because these are actually the least significant bits
        return hashIndexCodec.getFirst(keyHash);
    }

    /**
     * Compare a key with a serialized key that is pointed by a specific entry index
     *
     * @param tempKeyBuff a reusable buffer object for internal temporary usage
     *                    As a side effect, this buffer will contain the compared
     *                    serialized key.
     * @param key         the key to compare
     * @param ei          the entry index to compare with
     * @return the comparison result
     */
    int compareKeyAndEntryIndex(KeyBuffer tempKeyBuff, K key, int ei) {
        boolean isAllocated = entryHashSet.readKey(tempKeyBuff, ei);
        assert isAllocated;
        return comparator.compareKeyAndSerializedKey(key, tempKeyBuff);
    }

    /**
     * Look up a key in this chunk.
     *
     * @param ctx The context that follows the operation following this key look up.
     *            It will describe the state of the entry (key and value) associated with the input {@code key}.
     *            Following are the possible states of the entry:
     *             (1) {@code key} was not found.
     *                   This means there is no entry with the this key in this chunk.
     *                   In this case, {@code ctx.isKeyValid() == False} and {@code ctx.isValueValid() == False}.
     *             (2) {@code key} was found.
     *                   In this case, {@code (ctx.isKeyValid() == True}
     *                   The state of the value associated with {@code key} is described in {@code ctx.entryState}.
     *                   It can be one of the following states:
     *                     1- not yet inserted, 2- valid, 3- in the process of being deleted, 4- deleted.
     *
     *                   For cases (2) and (3), {@code ctx.isValueValid() == True}.
     *                   Otherwise, {@code ctx.isValueValid() == False}.
     *                   This means that there is an entry with that key, but there is no value attached to this key.
     *                   Such entry can be reused after finishing the deletion process, if needed.
     * @param key the key to look up
     */
    void lookUp(ThreadContext ctx, K key) {
        int keyHash = calculateKeyHash(key, ctx);
        entryHashSet.lookUp(ctx, key, calculateEntryIdx(key, keyHash), keyHash);
    }

    /********************************************************************************************/
    /*---------- Methods for managing the put/remove path of the keys and values  --------------*/

    void writeTemporaryKey(K key, KeyBuffer tempBuffer) {
        entryHashSet.writeKey(key, tempBuffer);
    }

    /**
     * As written in {@code writeValueFinish(ctx)}, when changing an entry, the value reference is CASed first and
     * later the value version, and the same applies when removing a value. However, there is another step before
     * changing an entry to remove a value and it is marking the value off-heap (the LP). This function is used to
     * first CAS the value reference to {@code INVALID_VALUE_REFERENCE} and then CAS the version to be a negative one.
     * Other threads seeing a marked value call this function before they proceed (e.g., before performing a
     * successful {@code putIfAbsent()}).
     *
     * @param ctx The context that follows the operation since the key was found/created.
     *            Holds the entry to change, the old value reference to CAS out, and the current value version.
     * @return true if a rebalance is needed
     * IMPORTANT: whether deleteValueFinish succeeded to mark the entry's value reference as
     * deleted, or not, if there were no request to rebalance FALSE is going to be returned
     */
    @Override
    boolean finalizeDeletion(ThreadContext ctx) {
        if (ctx.entryState != EntryArray.EntryState.DELETED_NOT_FINALIZED) {
            return false;
        }
        if (!publish()) {
            return true;
        }
        try {
            if (!entryHashSet.deleteValueFinish(ctx)) {
                return false;
            }
            externalSize.decrementAndGet();
            statistics.decrementAddedCount();
            return false;
        } finally {
            unpublish();
        }
    }

    /**
     * This function does the physical CAS of the value reference, which is the LP of the insertion. It then tries to
     * complete the insertion @see writeValueFinish(ctx).
     * This is also the only place in which the size of Oak is updated.
     *
     * @param ctx The context that follows the operation since the key was found/created.
     *            Holds the entry to which the value reference is linked, the old and new value references and
     *            the old and new value versions.
     * @return true if the value reference was CASed successfully.
     */
    @Override
    ValueUtils.ValueResult linkValue(ThreadContext ctx) {
        if (entryHashSet.writeValueCommit(ctx) == ValueUtils.ValueResult.FALSE) {
            return ValueUtils.ValueResult.FALSE;
        }

        // If we move a value, the statistics shouldn't change
        if (!ctx.isNewValueForMove) {
            statistics.incrementAddedCount();
            externalSize.incrementAndGet();
        }
        return ValueUtils.ValueResult.TRUE;
    }


    /********************************************************************************************/
    /*------------------------- Methods that are used for rebalance  ---------------------------*/
    @Override
    boolean shouldRebalance() {
        //TODO: no rebalance for now, add later
        return false;
    }

    /**
     * Copies entries from srcOrderedChunk (starting srcEntryIdx) to this chunk,
     * performing entries sorting on the fly (delete entries that are removed as well).
     *
     * @param tempValue   a reusable buffer object for internal temporary usage
     * @param srcOrderedChunk    chunk to copy from
     * @param srcEntryIdx start position for copying
     * @param maxCapacity max number of entries "this" chunk can contain after copy
     * @return entry index of next to the last copied entry (in the srcOrderedChunk),
     *         NONE_NEXT if all items were copied
     */
    final int copyPartOfEntries(
        ValueBuffer tempValue, HashChunk<K, V> srcOrderedChunk, final int srcEntryIdx, int maxCapacity) {

        //TODO: add rebalance code here
        return 0;
    }

    /********************************************************************************************/
    /*--------------------------------- Iterators Constructors ---------------------------------*/
    // TODO: update hash iterator later

}
