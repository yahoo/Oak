/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.util.EmptyStackException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

class Chunk<K, V> {
    static final int NONE_NEXT = 0;    // an entry with NONE_NEXT as its next pointer, points to a null entry

    /*-------------- Constants --------------*/

    enum State {
        INFANT,
        NORMAL,
        FROZEN,
        RELEASED
    }

    // used for checking if rebalance is needed
    private static final double REBALANCE_PROB_PERC = 30;
    private static final double SORTED_REBALANCE_RATIO = 2;
    private static final double MAX_ENTRIES_FACTOR = 2;
    private static final double MAX_IDLE_ENTRIES_FACTOR = 5;
    private static final int INVALID_ANCHOR_INDEX = -1;

    // defaults
    public static final int MAX_ITEMS_DEFAULT = 4096;

    /*-------------- Members --------------*/

    private static final Unsafe UNSAFE = UnsafeUtils.unsafe;
    KeyBuffer minKey;       // minimal key that can be put in this chunk
    AtomicMarkableReference<Chunk<K, V>> next;
    OakComparator<K> comparator;

    // in split/compact process, represents parent of split (can be null!)
    private final AtomicReference<Chunk<K, V>> creator;
    // chunk can be in the following states: normal, frozen or infant(has a creator)
    private final AtomicReference<State> state;
    private final AtomicReference<Rebalancer<K, V>> rebalancer;
    private final EntrySet<K, V> entrySet;

    private final AtomicInteger pendingOps;

    private final Statistics statistics;
    // # of sorted items at entry-array's beginning (resulting from split)
    private final AtomicInteger sortedCount;
    private final int maxItems;
    AtomicInteger externalSize; // for updating oak's size (reference to one global per Oak size)

    /*-------------- Constructors --------------*/

    /**
     * This constructor is only used internally to instantiate a Chunk without a creator and a min-key.
     * The caller should set the creator and min-key before returning the Chunk to the user.
     */
    private Chunk(int maxItems, AtomicInteger externalSize, MemoryManager vMM, MemoryManager kMM,
        OakComparator<K> comparator, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
        ValueUtils valueOperator) {
        this.maxItems = maxItems;
        this.externalSize = externalSize;
        this.comparator = comparator;
        this.entrySet =
            new EntrySet<>(vMM, kMM, maxItems, keySerializer, valueSerializer, valueOperator);
        // if not zero, sorted count keeps the entry index of the last
        // subsequent and ordered entry in the entries array
        this.sortedCount = new AtomicInteger(0);
        this.minKey = new KeyBuffer(kMM.getEmptySlice());
        this.creator = new AtomicReference<>(null);
        this.state = new AtomicReference<>(State.NORMAL);
        this.next = new AtomicMarkableReference<>(null, false);
        this.pendingOps = new AtomicInteger();
        this.rebalancer = new AtomicReference<>(null); // to be updated on rebalance
        this.statistics = new Statistics();
    }

    /**
     * This constructor is only used when creating the first chunk (without a creator).
     */
    Chunk(K minKey, int maxItems, AtomicInteger externalSize, MemoryManager vMM, MemoryManager kMM,
        OakComparator<K> comparator, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
        ValueUtils valueOperator) {

        this(maxItems, externalSize, vMM, kMM, comparator, keySerializer, valueSerializer, valueOperator);
        entrySet.allocateKey(minKey, this.minKey);
    }

    /**
     * Create a child Chunk where this Chunk object as its creator.
     * The child Chunk will have the same minKey as this Chunk (without duplicating the KeyBuffer data).
     */
    Chunk<K, V> createFirstChild() {
        Chunk<K, V> child =
            new Chunk<>(maxItems, externalSize,
                entrySet.valuesMemoryManager, entrySet.keysMemoryManager,
                comparator, entrySet.keySerializer, entrySet.valueSerializer,
                entrySet.valOffHeapOperator);
        child.creator.set(this);
        child.state.set(State.INFANT);
        child.minKey.copyFrom(this.minKey);
        return child;
    }

    /**
     * Create a child Chunk where this Chunk object as its creator.
     * The child Chunk will use a duplicate minKey of the input (allocates a new buffer).
     */
    Chunk<K, V> createNextChild(KeyBuffer minKey) {
        Chunk<K, V> child = new Chunk<>(maxItems, externalSize,
            entrySet.valuesMemoryManager, entrySet.keysMemoryManager,
            comparator, entrySet.keySerializer, entrySet.valueSerializer,
            entrySet.valOffHeapOperator);
        child.creator.set(this);
        child.state.set(State.INFANT);
        entrySet.duplicateKey(minKey, child.minKey);
        return child;
    }

    /********************************************************************************************/
    /*-----------------------------  Wrappers for EntrySet methods -----------------------------*/

    /**
     * See {@code EntrySet.isValueRefValidAndNotDeleted(int)} for more information
     */
    boolean isValueRefValid(int ei) {
        return entrySet.isValueRefValidAndNotDeleted(ei);
    }

    /**
     * See {@code EntrySet.readKey(ThreadContext)} for more information
     */
    void readKey(ThreadContext ctx) {
        entrySet.readKey(ctx);
    }

    /**
     * See {@code EntrySet.readValue(ThreadContext)} for more information
     */
    void readValue(ThreadContext ctx) {
        entrySet.readValue(ctx);
    }

    /**
     * See {@code EntrySet.readKey(KeyBuffer)} for more information
     */
    boolean readKeyFromEntryIndex(KeyBuffer key, int ei) {
        return entrySet.readKey(key, ei);
    }

    /**
     * See {@code EntrySet.readValue(ValueBuffer)} for more information
     */
    boolean readValueFromEntryIndex(ValueBuffer value, int ei) {
        return entrySet.readValue(value, ei);
    }

    /**
     * See {@code EntrySet.allocateEntry(ThreadContext)} for more information
     */
    boolean allocateEntry(ThreadContext ctx, K key) {
        return entrySet.allocateEntry(ctx, key);
    }

    /**
     * See {@code EntrySet.writeValueStart(ThreadContext)} for more information
     */
    void writeValue(ThreadContext ctx, V value, boolean writeForMove) {
        entrySet.writeValueStart(ctx, value, writeForMove);
    }

    /**
     * See {@code EntrySet.releaseKey(ThreadContext)} for more information
     */
    void releaseKey(ThreadContext ctx) {
        entrySet.releaseKey(ctx);
    }

    /**
     * See {@code EntrySet.releaseNewValue(ThreadContext)} for more information
     */
    void releaseNewValue(ThreadContext ctx) {
        entrySet.releaseNewValue(ctx);
    }

    /**
     * @param key a key buffer to be updated with the minimal key
     * @return true if successful
     */
    boolean readMinKey(KeyBuffer key) {
        return entrySet.readKey(key, entrySet.getHeadNextIndex());
    }

    /**
     * @param key a key buffer to be updated with the maximal key
     * @return true if successful
     */
    boolean readMaxKey(KeyBuffer key) {
        return entrySet.readKey(key, getLastItemEntryIndex());
    }

    /**
     * @return the index of the first item in the chunk
     * See {@code EntrySet.getHeadNextIndex} for more information.
     */
    final int getFirstItemEntryIndex() {
        return entrySet.getHeadNextIndex();
    }

    /**
     * Finds the last sorted entry.
     *
     * @return the last sorted entry
     */
    private int getLastItemEntryIndex() {
        int sortedCount = this.sortedCount.get();
        int entryIndex = sortedCount == 0 ? entrySet.getHeadNextIndex() : sortedCount;
        int nextEntryIndex = entrySet.getNextEntryIndex(entryIndex);
        while (nextEntryIndex != NONE_NEXT) {
            entryIndex = nextEntryIndex;
            nextEntryIndex = entrySet.getNextEntryIndex(entryIndex);
        }
        return entryIndex;
    }


    /********************************************************************************************/
    /*-----------------------  Methods for looking up item in this chunk -----------------------*/

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
        boolean isAllocated = entrySet.readKey(tempKeyBuff, ei);
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
     *                   The state of the value associated with {@code key} is described in {@code ctx.valueState}.
     *                   It can be one of the following states:
     *                     (1) not yet inserted, (2) in the process of being inserted, (3) valid,
     *                     (4) in the process of being deleted, (5) deleted.
     *                   For cases (2) and (3), {@code ctx.isValueValid() == True}.
     *                   Otherwise, {@code ctx.isValueValid() == False}.
     *                   This means that there is an entry with that key, but there is no value attached to this key.
     *                   Such entry can be reused after finishing the deletion process, if needed.
     * @param key the key to look up
     */
    void lookUp(ThreadContext ctx, K key) {
        // binary search sorted part of key array to quickly find node to start search at
        // it finds previous-to-key
        int curr = binaryFind(ctx.tempKey, key);
        curr = (curr == NONE_NEXT) ? entrySet.getHeadNextIndex() : entrySet.getNextEntryIndex(curr);

        // iterate until end of list (or key is found)
        while (curr != NONE_NEXT) {
            // compare current item's key to searched key
            int cmp = compareKeyAndEntryIndex(ctx.key, key, curr);
            // if item's key is larger - we've exceeded our key
            // it's not in chunk - no need to search further
            if (cmp < 0) {
                // Reset entry context to be INVALID
                ctx.invalidate();
                return;
            } else if (cmp == 0) { // if keys are equal - we've found the item
                // Updates the entry's context
                // ctx.key was already updated as a side effect of compareKeyAndEntryIndex()
                ctx.entryIndex = curr;
                entrySet.readValue(ctx);
                return;
            }
            // otherwise- proceed to next item
            curr = entrySet.getNextEntryIndex(curr);
        }

        // Reset entry context to be INVALID
        ctx.invalidate();
    }

    /**
     * binary search for largest-entry smaller than 'key' in sorted part of key array.
     *
     * @param tempKey a reusable buffer object for internal temporary usage
     * @param key     the key to look up
     * @return the index of the entry from which to start a linear search -
     * if key is found, its previous entry is returned!
     * In cases when search from the head is needed, meaning:
     * (1) the given key is less or equal than the smallest key in the chunk OR
     * (2) entries are unsorted so there is a need to start from the beginning of the linked list
     * NONE_NEXT is going to be returned
     */
    private int binaryFind(KeyBuffer tempKey, K key) {
        int sortedCount = this.sortedCount.get();
        // if there are no sorted keys, return NONE_NEXT to indicate that a regular linear search is needed
        if (sortedCount == 0) {
            return NONE_NEXT;
        }

        // if the first item is already larger than key,
        // return NONE_NEXT to indicate that a regular linear search is needed
        if (compareKeyAndEntryIndex(tempKey, key, entrySet.getHeadNextIndex()) <= 0) {
            return NONE_NEXT;
        }

        // optimization: compare with last key to avoid binary search (here sortedCount is not zero)
        if (compareKeyAndEntryIndex(tempKey, key, sortedCount) > 0) {
            return sortedCount;
        }

        int start = 0;
        int end = sortedCount;
        while (end - start > 1) {
            int curr = start + (end - start) / 2;
            if (compareKeyAndEntryIndex(tempKey, key, curr) <= 0) {
                end = curr;
            } else {
                start = curr;
            }
        }

        return start;
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
        if (ctx.valueState != EntrySet.ValueState.DELETED_NOT_FINALIZED) {
            return false;
        }
        if (!publish()) {
            return true;
        }
        try {
            if (!entrySet.deleteValueFinish(ctx)) {
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
     * @param ctx the context that follows the operation since the key was found/created
     * @param key the key to link
     * @return The previous entry index if the key was already added by another thread.
     *         Otherwise, if successful, it will return the current entry index.
     */
    int linkEntry(ThreadContext ctx, K key) {
        int prev;
        int curr;
        int cmp;
        int anchor = INVALID_ANCHOR_INDEX;
        final int ei = ctx.entryIndex;
        final KeyBuffer tempKeyBuff = ctx.tempKey;
        while (true) {
            // start iterating from quickly-found node (by binary search) in sorted part of order-array
            if (anchor == INVALID_ANCHOR_INDEX) {
                anchor = binaryFind(tempKeyBuff, key);
            }
            if (anchor == NONE_NEXT) {
                prev = NONE_NEXT;
                curr = entrySet.getHeadNextIndex();
            } else {
                prev = anchor;
                curr = entrySet.getNextEntryIndex(anchor);    // index of next item in list
            }

            //TODO: use ctx and location window inside ctx (when key wasn't found),
            //TODO: so there us no need to iterate again in linkEntry
            // iterate items until key's position is found
            while (true) {
                // if no item, done searching - add to end of list
                if (curr == NONE_NEXT) {
                    break;
                }
                // compare current item's key to ours
                cmp = compareKeyAndEntryIndex(tempKeyBuff, key, curr);

                // if current item's key is larger, done searching - add between prev and curr
                if (cmp < 0) {
                    break;
                }

                // if same key, someone else managed to add the key to the linked list
                if (cmp == 0) {
                    return curr;
                }

                prev = curr;
                curr = entrySet.getNextEntryIndex(prev);    // index of next item in list
            }

            // link to list between curr and previous, first change this entry's next to point to curr
            // no need for CAS since put is not even published yet
            entrySet.setNextEntryIndex(ei, curr);
            if (entrySet.casNextEntryIndex(prev, curr, ei)) {
                // Here is the single place where we do enter a new entry to the chunk, meaning
                // there is none else who can simultaneously insert the same key
                // (we were the first to insert this key).
                // If the new entry's index is exactly after the sorted count and
                // the entry's key is greater or equal then to the previous (sorted count)
                // index key. Then increase the sorted count.
                int sortedCount = this.sortedCount.get();
                if (sortedCount > 0) {
                    if (ei == (sortedCount + 1)) { // first entry has entry index 1, not 0
                        // the new entry's index is exactly after the sorted count
                        if (compareKeyAndEntryIndex(tempKeyBuff, key, sortedCount) >= 0) {
                            // compare with sorted count key, if inserting the "if-statement",
                            // the sorted count key is less or equal to the key just inserted
                            this.sortedCount.compareAndSet(sortedCount, (sortedCount + 1));
                        }
                    }
                }
                return ei;
            }
            // CAS didn't succeed, try again
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
        if (entrySet.writeValueCommit(ctx) == ValueUtils.ValueResult.FALSE) {
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
        // perform actual check only in pre defined percentage of puts
        if (ThreadLocalRandom.current().nextInt(100) > REBALANCE_PROB_PERC) {
            return false;
        }

        // if another thread already runs rebalance -- skip it
        if (!isEngaged(null)) {
            return false;
        }
        int numOfEntries = entrySet.getNumOfEntries();
        int numOfItems = statistics.getCompactedCount();
        int sortedCount = this.sortedCount.get();
        // Reasons for executing a rebalance:
        // 1. There are no sorted keys and the total number of entries is above a certain threshold.
        // 2. There are sorted keys, but the total number of unsorted keys is too big.
        // 3. Out of the occupied entries, there are not enough actual items.
        return (sortedCount == 0 && (numOfEntries * MAX_ENTRIES_FACTOR) > maxItems)
                || (sortedCount > 0 && (sortedCount * SORTED_REBALANCE_RATIO) < numOfEntries)
                || ((numOfEntries * MAX_IDLE_ENTRIES_FACTOR) > maxItems
                    && (numOfItems * MAX_IDLE_ENTRIES_FACTOR) < numOfEntries);
    }

    /**
     * Copies entries from srcChunk (starting srcEntryIdx) to this chunk,
     * performing entries sorting on the fly (delete entries that are removed as well).
     *
     * @param tempValue   a reusable buffer object for internal temporary usage
     * @param srcChunk    chunk to copy from
     * @param srcEntryIdx start position for copying
     * @param maxCapacity max number of entries "this" chunk can contain after copy
     * @return entry index of next to the last copied entry (in the srcChunk),
     *         NONE_NEXT if all items were copied
     */
    final int copyPartNoKeys(ValueBuffer tempValue, Chunk<K, V> srcChunk, final int srcEntryIdx, int maxCapacity) {

        if (srcEntryIdx == NONE_NEXT) {
            return NONE_NEXT;
        }

        // use local variables and just set the atomic variables once at the end
        int numOfEntries = entrySet.getNumOfEntries();
        // next *free* index of this entries array
        int sortedThisEntryIndex = numOfEntries + 1;

        // check that we are not beyond allowed number of entries to copy from source chunk
        if (numOfEntries >= maxCapacity) {
            return srcEntryIdx;
        }
        // assuming that all chunks are bounded with same number of entries to hold
        assert srcEntryIdx <= maxItems;

        // set the next entry index (previous entry or head) from where we start to copy
        // if sortedThisEntryIndex is one (first entry to be occupied on this chunk)
        // we are exactly going to update the head (ei=0)
        entrySet.setNextEntryIndex(sortedThisEntryIndex - 1, sortedThisEntryIndex);

        // Here was the code that was trying to read entries from srcEntryIdx on the source chunk
        // to see how much of them are subject for a copy, ordered and not deleted,
        // so theoretically they can be copied with copy array. The code is removed, because anyway
        // the long copy array doesn't happen since "next" needs to be updated separately.

        // copy entry by entry traversing the source linked list
        int curEntryIdx = srcEntryIdx;
        while (entrySet.copyEntry(tempValue, srcChunk.entrySet, curEntryIdx)) {
            // the source entry was either copied or disregarded as deleted
            // anyway move to next source entry (according to the linked list)
            curEntryIdx = srcChunk.entrySet.getNextEntryIndex(curEntryIdx);

            // if entry was ignored as deleted (no change in this EntrySet num of entries), continue
            if (numOfEntries == entrySet.getNumOfEntries()) {
                continue;
            }

            // we indeed copied the entry, update the number of entries and the next pointer
            numOfEntries++;
            sortedThisEntryIndex++;
            entrySet.setNextEntryIndex(sortedThisEntryIndex - 1, sortedThisEntryIndex);

            // check that we are not beyond allowed number of entries to copy from source chunk
            if (numOfEntries >= maxCapacity) {
                break;
            }

            // is there something to copy on the source side?
            if (curEntryIdx == NONE_NEXT) {
                break;
            }
        }
        // we have stopped the copy because (1) this entry set is full, OR (2) ended source entries,
        // OR (3) we copied allowed number of entries

        // the last next pointer was set to what is there in the source to copy, reset it to null
        entrySet.setNextEntryIndex(sortedThisEntryIndex - 1, NONE_NEXT);
        // sorted count keeps the number of sorted entries
        sortedCount.set(numOfEntries);
        statistics.updateInitialSortedCount(sortedCount.get());

        // check the validity of the new entrySet
        assert entrySet.isEntrySetValidAfterRebalance();

        return curEntryIdx; // if NONE_NEXT then we finished copying old chunk, else we reached max in new chunk
    }


    /********************************************************************************************/
    /*----------------------- Methods for managing the chunk's state  --------------------------*/

    State state() {
        return state.get();
    }

    Chunk<K, V> creator() {
        return creator.get();
    }

    private void setState(State state) {
        this.state.set(state);
    }

    void normalize() {
        state.compareAndSet(State.INFANT, State.NORMAL);
        creator.set(null);
        // using fence so other puts can continue working immediately on this chunk
        Chunk.UNSAFE.storeFence();
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

    /**
     * marks this chunk's next pointer so this chunk is marked as deleted
     *
     * @return the next chunk pointed to once marked (will not change)
     */
    Chunk<K, V> markAndGetNext() {
        // new chunks are ready, we mark frozen chunk's next pointer so it won't change
        // since next pointer can be changed by other split operations we need to do this in a loop - until we succeed
        while (true) {
            // if chunk is marked - that is ok and its next pointer will not be changed anymore
            // return whatever chunk is set as next
            if (next.isMarked()) {
                return next.getReference();
            } else { // otherwise try to mark it
                // read chunk's current next
                Chunk<K, V> savedNext = next.getReference();

                // try to mark next while keeping the same next chunk - using CAS
                // if we succeeded then the next pointer we remembered is set and will not change - return it
                if (next.compareAndSet(savedNext, savedNext, false, true)) {
                    return savedNext;
                }
            }
        }
    }


    /*-------------- Iterators --------------*/

    AscendingIter ascendingIter() {
        return new AscendingIter();
    }

    AscendingIter ascendingIter(ThreadContext ctx, K from, boolean inclusive) {
        return new AscendingIter(ctx, from, inclusive);
    }

    DescendingIter descendingIter(ThreadContext ctx) {
        return new DescendingIter(ctx);
    }

    DescendingIter descendingIter(ThreadContext ctx, K from, boolean inclusive) {
        return new DescendingIter(ctx, from, inclusive);
    }

    private int advanceNextIndex(final int entryIndex) {
        int next = entryIndex;
        while (next != NONE_NEXT && !entrySet.isValueRefValidAndNotDeleted(next)) {
            next = entrySet.getNextEntryIndex(next);
        }
        return next;
    }

    interface ChunkIter {
        boolean hasNext();

        int next(ThreadContext ctx);
    }

    class AscendingIter implements ChunkIter {

        private int next;

        AscendingIter() {
            next = entrySet.getHeadNextIndex();
            next = advanceNextIndex(next);
        }

        AscendingIter(ThreadContext ctx, K from, boolean inclusive) {
            KeyBuffer tempKeyBuff = ctx.tempKey;
            next = binaryFind(tempKeyBuff, from);
            next = (next == NONE_NEXT) ? entrySet.getHeadNextIndex() : entrySet.getNextEntryIndex(next);
            int compare = -1;
            if (next != NONE_NEXT) {
                compare = compareKeyAndEntryIndex(tempKeyBuff, from, next);
            }
            while (next != NONE_NEXT &&
                    (compare > 0 || (compare >= 0 && !inclusive) || !entrySet.isValueRefValidAndNotDeleted(next))) {
                next = entrySet.getNextEntryIndex(next);
                if (next != NONE_NEXT) {
                    compare = compareKeyAndEntryIndex(tempKeyBuff, from, next);
                }
            }
        }

        private void advance() {
            next = entrySet.getNextEntryIndex(next);
            next = advanceNextIndex(next);
        }

        @Override
        public boolean hasNext() {
            return next != NONE_NEXT;
        }

        @Override
        public int next(ThreadContext ctx) {
            int toReturn = next;
            advance();
            return toReturn;
        }
    }

    class DescendingIter implements ChunkIter {

        private int next;
        private int anchor;
        private int prevAnchor;
        private final IntStack stack;
        private final K from;
        private boolean inclusive;

        private final int skipEntriesForBiggerStack = (maxItems/10); // 1 is the lowest possible value

        DescendingIter(ThreadContext ctx) {
            KeyBuffer tempKeyBuff = ctx.tempKey;

            from = null;
            stack = new IntStack(entrySet.getLastEntryIndex());
            int sortedCnt = sortedCount.get();
            anchor = // this is the last sorted entry
                    (sortedCnt == 0 ? entrySet.getHeadNextIndex() : sortedCnt);
            stack.push(anchor);
            initNext(tempKeyBuff);
        }

        DescendingIter(ThreadContext ctx, K from, boolean inclusive) {
            KeyBuffer tempKeyBuff = ctx.tempKey;

            this.from = from;
            this.inclusive = inclusive;
            stack = new IntStack(entrySet.getLastEntryIndex());
            anchor = binaryFind(tempKeyBuff, from);
            // translate to be valid index, if anchor is head we know to stop the iteration
            anchor = (anchor == NONE_NEXT) ? entrySet.getHeadNextIndex() : anchor;
            stack.push(anchor);
            initNext(tempKeyBuff);
        }

        private void initNext(KeyBuffer keyBuff) {
            traverseLinkedList(keyBuff, true);
            advance(keyBuff);
        }

        /**
         * use stack to find a valid next, removed items can't be next
         */
        private void findNewNextInStack() {
            if (stack.empty()) {
                next = NONE_NEXT;
                return;
            }
            next = stack.pop();
            while (next != NONE_NEXT && !entrySet.isValueRefValidAndNotDeleted(next)) {
                if (!stack.empty()) {
                    next = stack.pop();
                } else {
                    next = NONE_NEXT;
                    return;
                }
            }
        }

        private void pushToStack(boolean compareWithPrevAnchor) {
            while (next != NONE_NEXT) {
                if (!compareWithPrevAnchor) {
                    stack.push(next);
                    next = entrySet.getNextEntryIndex(next);
                } else {
                    if (next != prevAnchor) {
                        stack.push(next);
                        next = entrySet.getNextEntryIndex(next);
                    } else {
                        break;
                    }
                }
            }
        }

        /**
         * fill the stack
         *
         * @param firstTimeInvocation
         */
        private void traverseLinkedList(KeyBuffer tempKeyBuff, boolean firstTimeInvocation) {
            assert stack.size() == 1;   // ancor is in the stack
            if (prevAnchor == entrySet.getNextEntryIndex(anchor)) {
                next = NONE_NEXT;   // there is no next;
                return;
            }
            next = entrySet.getNextEntryIndex(anchor);
            if (from == null) {
                // if this is not the first invocation, stop when reaching previous anchor
                pushToStack(!firstTimeInvocation);
            } else {
                if (firstTimeInvocation) {
                    final int threshold = inclusive ? -1 : 0;
                    // This is equivalent to continue while:
                    //         when inclusive: CMP >= 0
                    //     when non-inclusive: CMP > 0
                    while (next != NONE_NEXT && compareKeyAndEntryIndex(tempKeyBuff, from, next) > threshold) {
                        stack.push(next);
                        next = entrySet.getNextEntryIndex(next);
                    }
                } else {
                    // stop when reaching previous anchor
                    pushToStack(true);
                }
            }
        }

        /**
         * find new valid anchor
         */
        private void findNewAnchor() {
            assert stack.empty();
            prevAnchor = anchor;
            if (anchor == entrySet.getHeadNextIndex()) {
                next = NONE_NEXT; // there is no more in this chunk
                return;
            } else if (anchor == 1) { // cannot get below the first index
                anchor = entrySet.getHeadNextIndex();
            } else {
                if ((anchor - skipEntriesForBiggerStack) > 1) {
                    // try to skip more then one backward step at a time
                    // if it shows better performance
                    anchor -= skipEntriesForBiggerStack;
                } else {
                    anchor -= 1;
                }
            }
            stack.push(anchor);
        }

        private void advance(KeyBuffer keyBuff) {
            while (true) {
                findNewNextInStack();
                if (next != NONE_NEXT) {
                    return;
                }
                // there is no next in stack
                if (anchor == entrySet.getHeadNextIndex()) {
                    // there is no next at all
                    return;
                }
                findNewAnchor();
                traverseLinkedList(keyBuff, false);
            }
        }

        @Override
        public boolean hasNext() {
            return next != NONE_NEXT;
        }

        @Override
        public int next(ThreadContext ctx) {
            int toReturn = next;
            advance(ctx.tempKey);
            return toReturn;
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
