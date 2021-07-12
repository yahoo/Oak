/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.EmptyStackException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class HashChunk<K, V> {
    // HashChunk takes a number of least significant bits from the full key hash
    // to provide as an index in the EntryHashSet
    private static final int DEFAULT_MAX_LSB_SECOND_LEVEL = 16;
    private static final int DEFAULT_MIN_LSB_SECOND_LEVEL = 4;
    private int lsbForSecondLevelHash = DEFAULT_MAX_LSB_SECOND_LEVEL;
    private UnionCodec hashIndexCodec;
    /*-------------- Constants --------------*/

    enum State {
        INFANT,
        NORMAL,
        FROZEN,
        RELEASED
    }

    // defaults
    public static final int MAX_HASH_ITEMS_DEFAULT = 4096;

    /*-------------- Members --------------*/
    OakComparator<K> comparator;

    // in split/compact process, represents parent of split (can be null!)
    private final AtomicReference<HashChunk<K, V>> creator;
    // chunk can be in the following states: normal, frozen or infant(has a creator)
    private final AtomicReference<State> state;
    private final AtomicReference<Rebalancer<K, V>> rebalancer;
    private final EntryHashSet<K, V> entryHashSet;

    private final AtomicInteger pendingOps;

    private final Statistics statistics;

    private final int maxItems;
    private AtomicInteger externalSize; // for updating oak's size (reference to one global per Oak size)

    /*-------------- Constructors --------------*/

    /**
     * This constructor is only used when creating the first ever chunks (without a creator).
     * The caller might set the creator before returning the OrderedChunk to the user.
     */
    HashChunk(int maxItems, AtomicInteger externalSize, MemoryManager vMM, MemoryManager kMM,
        OakComparator<K> comparator, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer) {
        this.maxItems = maxItems;
        this.externalSize = externalSize;
        this.comparator = comparator;
        this.entryHashSet =
            new EntryHashSet<>(vMM, kMM, maxItems, keySerializer, valueSerializer, comparator);
        this.creator = new AtomicReference<>(null);
        this.state = new AtomicReference<>(State.NORMAL);
        this.pendingOps = new AtomicInteger();
        this.rebalancer = new AtomicReference<>(null); // to be updated on rebalance
        this.statistics = new Statistics();
    }

    /**
     * Create a child OrderedChunk where this OrderedChunk object as its creator.
     * The child OrderedChunk will have the same minKey as this OrderedChunk (without duplicating the KeyBuffer data).
     */
    HashChunk<K, V> createFirstChild() {
        HashChunk<K, V> child =
            new HashChunk<>(maxItems, externalSize,
                entryHashSet.valuesMemoryManager, entryHashSet.keysMemoryManager,
                comparator, entryHashSet.keySerializer, entryHashSet.valueSerializer);
        child.creator.set(this);
        child.state.set(State.INFANT);
        // each child chunk is responsible for less hash values
        child.lsbForSecondLevelHash =
            this.lsbForSecondLevelHash > DEFAULT_MIN_LSB_SECOND_LEVEL ?
                this.lsbForSecondLevelHash - 1 : this.lsbForSecondLevelHash;
        this.hashIndexCodec = new UnionCodec(UnionCodec.INVALID_BIT_SIZE, lsbForSecondLevelHash);
        return child;
    }

    /**
     * Create a child OrderedChunk where this OrderedChunk object as its creator.
     * The child OrderedChunk will use a duplicate minKey of the input (allocates a new buffer).
     */
    HashChunk<K, V> createNextChild() {
        HashChunk<K, V> child = new HashChunk<>(maxItems, externalSize,
            entryHashSet.valuesMemoryManager, entryHashSet.keysMemoryManager,
            comparator, entryHashSet.keySerializer, entryHashSet.valueSerializer);
        child.creator.set(this);
        child.state.set(State.INFANT);
        // each child chunk is responsible for less hash values
        child.lsbForSecondLevelHash =
            this.lsbForSecondLevelHash > DEFAULT_MIN_LSB_SECOND_LEVEL ?
                this.lsbForSecondLevelHash - 1 : this.lsbForSecondLevelHash;
        this.hashIndexCodec = new UnionCodec(UnionCodec.INVALID_BIT_SIZE, lsbForSecondLevelHash);
        return child;
    }

    /********************************************************************************************/
    /*-----------------------------  Wrappers for EntryOrderedSet methods -----------------------------*/

    /**
     * See {@code EntryOrderedSet.isValueRefValidAndNotDeleted(int)} for more information
     */
    boolean isValueRefValidAndNotDeleted(int ei) {
        return entryHashSet.isValueRefValidAndNotDeleted(ei);
    }

    /**
     * See {@code EntryOrderedSet.readKey(ThreadContext)} for more information
     */
    void readKey(ThreadContext ctx) {
        entryHashSet.readKey(ctx);
    }

    /**
     * See {@code EntryOrderedSet.readValue(ThreadContext)} for more information
     */
    void readValue(ThreadContext ctx) {
        entryHashSet.readValue(ctx);
    }

    /**
     * See {@code EntryOrderedSet.readKey(KeyBuffer)} for more information
     */
    boolean readKeyFromEntryIndex(KeyBuffer key, int ei) {
        return entryHashSet.readKey(key, ei);
    }

    /**
     * See {@code EntryOrderedSet.readValue(ValueBuffer)} for more information
     */
    boolean readValueFromEntryIndex(ValueBuffer value, int ei) {
        return entryHashSet.readValue(value, ei);
    }

    /**
     * Writes the key off-heap and allocates an entry with the reference pointing to the given key
     * See {@code EntryOrderedSet.allocateEntryAndWriteKey(ThreadContext)} for more information
     */
    boolean allocateEntryAndWriteKey(ThreadContext ctx, K key) {
        //TODO: need to provide the hash function and calculate the bits
        int fullHash = key.hashCode();
        int hashIndex = 11; //TODO: Fix
        return entryHashSet.allocateEntryAndWriteKey(ctx, key, hashIndex, fullHash);
    }

    /**
     * See {@code EntryOrderedSet.allocateValue(ThreadContext)} for more information
     */
    void allocateValue(ThreadContext ctx, V value, boolean writeForMove) {
        entryHashSet.allocateValue(ctx, value, writeForMove);
    }

    /**
     * See {@code EntryOrderedSet.releaseKey(ThreadContext)} for more information
     */
    void releaseKey(ThreadContext ctx) {
        entryHashSet.releaseKey(ctx);
    }

    /**
     * See {@code EntryOrderedSet.releaseNewValue(ThreadContext)} for more information
     */
    void releaseNewValue(ThreadContext ctx) {
        entryHashSet.releaseNewValue(ctx);
    }

    /********************************************************************************************/
    /*-----------------------  Methods for looking up item in this chunk -----------------------*/

    private int calculateHashIdx(K key) {
        int hashNumber = key.hashCode();
        return 0;
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
        int hashNumber = key.hashCode();
        int hashIdx = hashNumber % lsbForSecondLevelHash; //TODO: to be changed
        entryHashSet.lookUp(ctx, key, hashIdx, hashNumber);
        return;
    }

    /********************************************************************************************/
    /*---------- Methods for managing the put/remove path of the keys and values  --------------*/

    /**
     * publish operation into thread array
     * if CAS didn't succeed then this means that a rebalancer got here first and entry is frozen
     *
     * @return result of CAS
     **/
    boolean publish() {
        pendingOps.incrementAndGet();
        State currentState = state.get();
        if (currentState == State.FROZEN || currentState == State.RELEASED) {
            pendingOps.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * unpublish operation from thread array
     * if CAS didn't succeed then this means that a rebalancer did this already
     **/
    void unpublish() {
        pendingOps.decrementAndGet();
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

    int getMaxItems() {
        return maxItems;
    }

    /**
     * Engage the chunk to a rebalancer r.
     *
     * @param r -- a rebalancer to engage with
     */
    void engage(Rebalancer<K, V> r) {
        rebalancer.compareAndSet(null, r);
    }

    /**
     * Checks whether the chunk is engaged with a given rebalancer.
     *
     * @param r -- a rebalancer object. If r is null, verifies that the chunk is not engaged to any rebalancer
     * @return true if the chunk is engaged with r, false otherwise
     */
    boolean isEngaged(Rebalancer<K, V> r) {
        return rebalancer.get() == r;
    }

    /**
     * Fetch a rebalancer engaged with the chunk.
     *
     * @return rebalancer object or null if not engaged.
     */
    Rebalancer<K, V> getRebalancer() {
        return rebalancer.get();
    }

    boolean shouldRebalance() {
        //TODO: add new code here
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

        //TODO: add new code here
        return 0;
    }


    /********************************************************************************************/
    /*----------------------- Methods for managing the chunk's state  --------------------------*/

    State state() {
        return state.get();
    }

    HashChunk<K, V> creator() {
        return creator.get();
    }

    private void setState(State state) {
        this.state.set(state);
    }

    void normalize() {
        state.compareAndSet(State.INFANT, State.NORMAL);
        creator.set(null);
        // using fence so other puts can continue working immediately on this chunk
        UnsafeUtils.UNSAFE.storeFence();
    }

    /**
     * freezes chunk so no more changes can be done to it (marks pending items as frozen)
     */
    void freeze() {
        setState(State.FROZEN); // prevent new puts to this chunk
        while (pendingOps.get() != 0) {
            assert Boolean.TRUE;
        }
    }

    /**
     * try to change the state from frozen to released
     */
    void release() {
        state.compareAndSet(State.FROZEN, State.RELEASED);
    }


    /********************************************************************************************/
    /*--------------------------------- Iterators Constructors ---------------------------------*/

    /**
     * Ascending iterator from the beginning of the chunk. The end boundary is given by parameter
     * key "to" might not be in this chunk. Parameter nextChunkMinKey - is the minimal key of the
     * next chunk, all current and future keys of this chunk are less than nextChunkMinKey
     */
    AscendingIter ascendingIter(ThreadContext ctx, K to, boolean toInclusive,
        OakScopedReadBuffer nextChunkMinKey) {
        return new AscendingIter(ctx, to, toInclusive, nextChunkMinKey);
    }

    /**
     * Ascending iterator from key "from" given as parameter.
     * The end boundary is given by parameter key "to", but it might not be in this chunk.
     * Parameter nextChunkMinKey - is the minimal key of the next chunk,
     * all current and future keys of this chunk are less than nextChunkMinKey
     */
    AscendingIter ascendingIter(ThreadContext ctx, K from, boolean fromInclusive, K to,
        boolean toInclusive, OakScopedReadBuffer nextChunkMinKey) {
        return new AscendingIter(ctx, from, fromInclusive, to, toInclusive, nextChunkMinKey);
    }

    /********************************************************************************************/
    /*------ Base Class for OrderedChunk Iterators (keeps all the common fields and methods) ----------*/

    // specifier whether the end boundary check needs to be performed on the current scan output
    enum IterEndBoundCheck {
        NEVER_END_BOUNDARY_CHECK,
        MID_END_BOUNDARY_CHECK,
        ALWAYS_END_BOUNDARY_CHECK
    }

    abstract class ChunkIter {
        protected int next;         // index of the next entry to be returned
        protected K endBound;       // stop bound key, or null if no stop bound
        protected boolean endBoundInclusive;  // inclusion flag for "to"

        protected IterEndBoundCheck isEndBoundCheckNeeded = IterEndBoundCheck.NEVER_END_BOUNDARY_CHECK;

        abstract boolean hasNext();

        /** Returns the index of the entry that should be returned next by the iterator.
         ** NONE_NEXT is returned when iterator came to its end.
         **/
        abstract int next(ThreadContext ctx);

        boolean isBoundCheckNeeded() {
            return isEndBoundCheckNeeded == IterEndBoundCheck.ALWAYS_END_BOUNDARY_CHECK;
        };

        /* Checks if the given 'boundKey' key is beyond the scope of the given scan,
        ** meaning that scan is near to its end.
        ** For descending scan it is the low key, for ascending scan it is the high.
        **/
        abstract boolean isKeyOutOfEndBound(OakScopedReadBuffer boundKey);

        protected void setIsEndBoundCheckNeeded(
            ThreadContext ctx, K to, boolean toInclusive, OakScopedReadBuffer chunkBoundaryKey) {
            this.endBound = to;
            this.endBoundInclusive = toInclusive;

            if (this.endBound == null || !isKeyOutOfEndBound(chunkBoundaryKey)) {
                // isEndBoundCheckNeeded is NEVER_END_BOUNDARY_CHECK by default
                return;
            }

            // generally there is a need for the boundary check, but maybe delay it to the middle
            // of the chunk. isEndBoundCheckNeeded value can still be changed
            isEndBoundCheckNeeded = IterEndBoundCheck.ALWAYS_END_BOUNDARY_CHECK;
        }
    }

    /********************************************************************************************/
    /*------------------------------- Iterators Implementations --------------------------------*/
    class AscendingIter extends ChunkIter {

        AscendingIter(ThreadContext ctx, K to, boolean toInclusive,
            OakScopedReadBuffer nextChunkMinKey) {
            next = 0;
            next = advanceNextIndexNoBound(next, ctx);
            setIsEndBoundCheckNeeded(ctx, to, toInclusive, nextChunkMinKey);
        }

        AscendingIter(ThreadContext ctx, K from, boolean fromInclusive, K to, boolean toInclusive,
            OakScopedReadBuffer nextChunkMinKey) {
            KeyBuffer tempKeyBuff = ctx.tempKey;
            next = 0;
        }

        private void advance(ThreadContext ctx) {
            next++;
            // if there is no need to check the end-boundary on this chunk (IterEndBoundCheck.NEVER_END_BOUNDARY_CHECK),
            // or if the caller will check the end-boundary (IterEndBoundCheck.ALWAYS_END_BOUNDARY_CHECK),
            // then advance next without additional checks
            if (isEndBoundCheckNeeded != IterEndBoundCheck.MID_END_BOUNDARY_CHECK) {
                advanceNextIndexNoBound(next, ctx);
            } else {
                next = advanceNextIndex(next, ctx);
            }
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public int next(ThreadContext ctx) {
            int toReturn = next;
            advance(ctx);
            return toReturn;
        }

        private int advanceNextIndex(final int entryIndex, ThreadContext ctx) {
            int next = entryIndex;
            while (!entryHashSet.isValueRefValidAndNotDeleted(next)) {
                next++;
            }
            return next;
        }

        private int advanceNextIndexNoBound(final int entryIndex, ThreadContext ctx) {
            int next = entryIndex;
            while (!entryHashSet.isValueRefValidAndNotDeleted(next)) {
                next++;
            }
            return next;
        }

        @Override
        protected boolean isKeyOutOfEndBound(OakScopedReadBuffer key) {
            if (endBound == null) {
                return false;
            }
            if (key == null) {
                // we are on the last chunk and 'to' is not null
                return true;
            }
            int c = comparator.compareKeyAndSerializedKey(endBound, key);
            // return true if endBound<key or endBound==key and the scan was not endBoundInclusive
            return c < 0 || (c == 0 && !endBoundInclusive);
        }
    }

    /**
     * just a simple stack of int, implemented with int array
     */

    static class IntStack {

        private final int[] stack;
        private int top;

        IntStack(int size) {
            stack = new int[size];
            top = 0;
        }

        void push(int i) {
            stack[top] = i;
            top++;
        }

        int pop() {
            if (empty()) {
                throw new EmptyStackException();
            }
            top--;
            return stack[top];
        }

        boolean empty() {
            return top == 0;
        }

        int size() {
            return top;
        }

    }

    /*-------------- Statistics --------------*/

    /**
     * This class contains information about chunk utilization.
     */
    static class Statistics {
        private final AtomicInteger addedCount = new AtomicInteger(0);
        private int initialSortedCount = 0;

        /**
         * Initial sorted count here is immutable after chunk re-balance
         */
        void updateInitialSortedCount(int sortedCount) {
            this.initialSortedCount = sortedCount;
        }

        /**
         * @return number of items chunk will contain after compaction.
         */
        int getCompactedCount() {
            return initialSortedCount + getAddedCount();
        }

        /**
         * Incremented when put a key that was removed before
         */
        void incrementAddedCount() {
            addedCount.incrementAndGet();
        }

        /**
         * Decrement when remove a key that was put before
         */
        void decrementAddedCount() {
            addedCount.decrementAndGet();
        }

        int getAddedCount() {
            return addedCount.get();
        }

    }

    /**
     * @return statistics object containing approximate utilization information.
     */
    Statistics getStatistics() {
        return statistics;
    }

}
