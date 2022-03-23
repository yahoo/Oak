/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.EmptyStackException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

class OrderedChunk<K, V> extends BasicChunk<K, V> {
    // an entry with NONE_NEXT as its next pointer, points to a null entry
    static final int NONE_NEXT = EntryArray.INVALID_ENTRY_INDEX;

    /*-------------- Constants --------------*/
    // used for checking if rebalance is needed
    private static final double REBALANCE_PROB_PERC = 30;
    private static final double SORTED_REBALANCE_RATIO = 2;
    private static final double MAX_ENTRIES_FACTOR = 2;
    private static final double MAX_IDLE_ENTRIES_FACTOR = 5;

    // defaults
    public static final int ORDERED_CHUNK_MAX_ITEMS_DEFAULT = 4096;

    /*-------------- Members --------------*/
    KeyBuffer minKey;       // minimal key that can be put in this chunk
    AtomicMarkableReference<OrderedChunk<K, V>> next;
    private final EntryOrderedSet<K, V> entryOrderedSet;

    // # of sorted items at entry-array's beginning (resulting from split)
    private final AtomicInteger sortedCount;

    /*-------------- Constructors --------------*/
    /**
     * This constructor is only used internally to instantiate a OrderedChunk without a creator and a min-key.
     * The caller should set the creator and min-key before returning the OrderedChunk to the user.
     */
    private OrderedChunk(OakSharedConfig<K, V> config, int maxItems) {
        super(config, maxItems);
        this.entryOrderedSet = new EntryOrderedSet<>(config, maxItems);
        // sortedCount keeps the number of  subsequent and ordered entries in the entries array,
        // which are subject to binary search
        this.sortedCount = new AtomicInteger(0);
        this.minKey = new KeyBuffer(config.keysMemoryManager.getEmptySlice());
        this.next = new AtomicMarkableReference<>(null, false);
    }

    /**
     * This constructor is only used when creating the first ever chunk (without a creator).
     */
    OrderedChunk(OakSharedConfig<K, V> config, K minKey, int maxItems) {
        this(config, maxItems);
        entryOrderedSet.writeKey(minKey, this.minKey);
    }

    /**
     * Create a child OrderedChunk where this OrderedChunk object as its creator.
     * The child OrderedChunk will have the same minKey as this OrderedChunk
     * (without duplicating the KeyBuffer data).
     */
    OrderedChunk<K, V> createFirstChild() {
        OrderedChunk<K, V> child = new OrderedChunk<>(config, maxItems);
        updateBasicChild(child);
        child.minKey.copyFrom(this.minKey);
        return child;
    }

    /**
     * Create a child OrderedChunk where this OrderedChunk object as its creator.
     * The child OrderedChunk will use a duplicate minKey of the input (allocates a new buffer).
     */
    OrderedChunk<K, V> createNextChild(KeyBuffer minKey) {
        OrderedChunk<K, V> child = new OrderedChunk<>(config, maxItems);
        updateBasicChild(child);
        duplicateKeyBuffer(minKey, child.minKey);
        return child;
    }

    /**
     * Allocate a new KeyBuffer and duplicate an existing key to the new one.
     *
     * @param src the off-heap KeyBuffer to copy from
     * @param dst the off-heap KeyBuffer to update with the new allocation
     */
    private void duplicateKeyBuffer(KeyBuffer src, KeyBuffer dst) {
        src.s.preWrite();
        final int keySize = src.capacity();
        dst.getSlice().allocate(keySize, false);

        // We duplicate the buffer without instantiating a write buffer because the user is not involved.
        DirectUtils.UNSAFE.copyMemory(src.getAddress(), dst.getAddress(), keySize);
        src.s.postWrite();
    }

    /********************************************************************************************/
    /*-----------------------------  Wrappers for EntryOrderedSet methods -----------------------------*/

    /**
     * See {@code EntryOrderedSet.isValueRefValidAndNotDeleted(int)} for more information
     */
    boolean isValueRefValidAndNotDeleted(int ei) {
        return entryOrderedSet.isValueRefValidAndNotDeleted(ei);
    }

    /**
     * See {@code EntryOrderedSet.readKey(ThreadContext)} for more information
     */
    void readKey(ThreadContext ctx) {
        entryOrderedSet.readKey(ctx);
    }

    /**
     * See {@code EntryOrderedSet.readValue(ThreadContext)} for more information
     */
    void readValue(ThreadContext ctx) {
        entryOrderedSet.readValue(ctx);
    }

    /**
     * See {@code EntryOrderedSet.readKey(KeyBuffer)} for more information
     */
    boolean readKeyFromEntryIndex(KeyBuffer key, int ei) {
        return entryOrderedSet.readKey(key, ei);
    }

    /**
     * See {@code EntryOrderedSet.readValue(ValueBuffer)} for more information
     */
    boolean readValueFromEntryIndex(ValueBuffer value, int ei) {
        return entryOrderedSet.readValue(value, ei);
    }

    /**
     * Writes the key off-heap and allocates an entry with the reference pointing to the given key
     * See {@code EntryOrderedSet.allocateEntryAndWriteKey(ThreadContext)} for more information
     */
    @Override
    boolean allocateEntryAndWriteKey(ThreadContext ctx, K key) {
        return entryOrderedSet.allocateEntryAndWriteKey(ctx, key);
    }

    /**
     * See {@code EntryOrderedSet.allocateValue(ThreadContext)} for more information
     */
    @Override
    void allocateValue(ThreadContext ctx, V value, boolean writeForMove) {
        entryOrderedSet.allocateValue(ctx, value, writeForMove);
    }

    /**
     * See {@code EntryOrderedSet.releaseKey(ThreadContext)} for more information
     */
    @Override
    void releaseKey(ThreadContext ctx) {
        entryOrderedSet.releaseKey(ctx);
    }

    /**
     * See {@code EntryOrderedSet.releaseNewValue(ThreadContext)} for more information
     */
    @Override
    void releaseNewValue(ThreadContext ctx) {
        entryOrderedSet.releaseNewValue(ctx);
    }
    
    @Override
    void release() {
        boolean released = state.compareAndSet(State.FROZEN, State.RELEASED);
        if ( !(config.keysMemoryManager instanceof SeqExpandMemoryManager) && released ) {
            entryOrderedSet.releaseAllDeletedKeys();
        }
    }

    /**
     * @param key a key buffer to be updated with the minimal key
     * @return true if successful
     */
    boolean readMinKey(KeyBuffer key) {
        return entryOrderedSet.readKey(key, entryOrderedSet.getHeadNextEntryIndex());
    }

    /**
     * @param key a key buffer to be updated with the maximal key
     * @return true if successful
     */
    boolean readMaxKey(KeyBuffer key) {
        return entryOrderedSet.readKey(key, getLastItemEntryIndex());
    }

    /**
     * @return the index of the first item in the chunk
     * See {@code EntryOrderedSet.getHeadNextIndex} for more information.
     */
    final int getFirstItemEntryIndex() {
        return entryOrderedSet.getHeadNextEntryIndex();
    }

    /**
     * Finds the last sorted entry.
     *
     * @return the last sorted entry
     */
    private int getLastItemEntryIndex() {
        int sortedCount = this.sortedCount.get();
        int entryIndex = sortedCount == 0 ?
            entryOrderedSet.getHeadNextEntryIndex() : getLastSortedEntryIndex(sortedCount);
        int nextEntryIndex = entryOrderedSet.getNextEntryIndex(entryIndex);
        while (nextEntryIndex != NONE_NEXT) {
            entryIndex = nextEntryIndex;
            nextEntryIndex = entryOrderedSet.getNextEntryIndex(entryIndex);
        }
        return entryIndex;
    }

    private int getLastSortedEntryIndex(int sortedCount) {
        return sortedCount - 1;
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
        boolean isAllocated = entryOrderedSet.readKey(tempKeyBuff, ei);
        if (!isAllocated) {
            throw new DeletedMemoryAccessException();
        }
        return KeyUtils.compareEntryKeyAndSerializedKey(key, tempKeyBuff, config.comparator);
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
        // binary search sorted part of key array to quickly find node to start search at
        // it finds previous-to-key
        int curr = binaryFind(ctx.tempKey, key);
        curr = (curr == NONE_NEXT) ? entryOrderedSet.getHeadNextEntryIndex() : entryOrderedSet.getNextEntryIndex(curr);

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
                entryOrderedSet.readValue(ctx);
                return;
            }
            // otherwise- proceed to next item
            curr = entryOrderedSet.getNextEntryIndex(curr);
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
        if (compareKeyAndEntryIndex(tempKey, key, entryOrderedSet.getHeadNextEntryIndex()) <= 0) {
            return NONE_NEXT;
        }

        // optimization: compare with last key to avoid binary search (here sortedCount is not zero)
        if (compareKeyAndEntryIndex(tempKey, key, getLastSortedEntryIndex(sortedCount)) > 0) {
            return getLastSortedEntryIndex(sortedCount);
        }

        // `start` and `end` are intentionally initiated outside of the array boundaries.
        // So the returned key index is exactly of the key we are looking for,
        // or of the maximal key still less than the key we are looking for...
        int start = -1;
        int end = sortedCount;
        while (end - start > 1) {
            int curr = start + ((end - start) / 2);
            if (compareKeyAndEntryIndex(tempKey, key, curr) <= 0) {
                end = curr;
            } else {
                start = curr;
            }
        }

        // If the key is below the first item, then return NONE_NEXT
        return start == -1 ? NONE_NEXT : start;
    }

    /*---------- Methods for managing the put/remove path of the keys and values  --------------*/
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
            if (!entryOrderedSet.deleteValueFinish(ctx)) {
                return false;
            }
            config.size.decrementAndGet();
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
        final int ei = ctx.entryIndex;
        final KeyBuffer tempKeyBuff = ctx.tempKey;

        // start iterating from quickly-found node (by binary search) in sorted part of order-array
        final int anchor = binaryFind(tempKeyBuff, key);
        while (true) {
            if (anchor == NONE_NEXT) {
                prev = NONE_NEXT;
                curr = entryOrderedSet.getHeadNextEntryIndex();
            } else {
                prev = anchor;
                curr = entryOrderedSet.getNextEntryIndex(anchor);    // index of next item in list
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
                curr = entryOrderedSet.getNextEntryIndex(prev);    // index of next item in list
            }

            // link to list between curr and previous, first change this entry's next to point to curr
            // no need for CAS since put is not even published yet
            entryOrderedSet.setNextEntryIndex(ei, curr);

            boolean linkSuccess;
            if (prev != NONE_NEXT) {
                linkSuccess = entryOrderedSet.casNextEntryIndex(prev, curr, ei);
            } else {
                linkSuccess = entryOrderedSet.casHeadEntryIndex(curr, ei);
            }
            if (linkSuccess) {
                // Here is the single place where we do enter a new entry to the chunk, meaning
                // there is none else who can simultaneously insert the same key
                // (we were the first to insert this key).
                // If the new entry's index is exactly after the sorted count and
                // the entry's key is greater or equal then to the previous (sorted count)
                // index key. Then increase the sorted count.
                int sortedCount = this.sortedCount.get();
                if (sortedCount > 0) {
                    if (ei == sortedCount) {
                        // the new entry's index is exactly after the sorted count
                        if (compareKeyAndEntryIndex(tempKeyBuff, key, getLastSortedEntryIndex(sortedCount)) >= 0) {
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
    @Override
    ValueUtils.ValueResult linkValue(ThreadContext ctx) {
        if (entryOrderedSet.writeValueCommit(ctx) == ValueUtils.ValueResult.FALSE) {
            return ValueUtils.ValueResult.FALSE;
        }

        // If we move a value, the statistics shouldn't change
        if (!ctx.isNewValueForMove) {
            statistics.incrementAddedCount();
            config.size.incrementAndGet();
        }
        return ValueUtils.ValueResult.TRUE;
    }

    /*------------------------- Methods that are used for rebalance  ---------------------------*/
    @Override
    boolean shouldRebalance() {
        // perform actual check only in pre defined percentage of puts
        if (ThreadLocalRandom.current().nextInt(100) > REBALANCE_PROB_PERC) {
            return false;
        }

        // if another thread already runs rebalance -- skip it
        if (!isEngaged(null)) {
            return false;
        }
        int numOfEntries = entryOrderedSet.getNumOfEntries();
        int numOfItems = statistics.getTotalCount();
        int sortedCount = this.sortedCount.get();
        // Reasons for executing a rebalance:
        // 1. There are no sorted keys and the total number of entries is above a certain threshold.
        // 2. There are sorted keys, but the total number of unsorted keys is too big.
        // 3. Out of the occupied entries, there are not enough actual items.
        return (sortedCount == 0 && (numOfEntries * MAX_ENTRIES_FACTOR) > getMaxItems())
                || (sortedCount > 0 && (sortedCount * SORTED_REBALANCE_RATIO) < numOfEntries)
                || ((numOfEntries * MAX_IDLE_ENTRIES_FACTOR) > getMaxItems()
                    && (numOfItems * MAX_IDLE_ENTRIES_FACTOR) < numOfEntries);
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
        ValueBuffer tempValue, OrderedChunk<K, V> srcOrderedChunk, final int srcEntryIdx, int maxCapacity) {

        if (srcEntryIdx == NONE_NEXT) {
            return NONE_NEXT;
        }

        // use local variables and just set the atomic variables once at the end
        int thisNumOfEntries = entryOrderedSet.getNumOfEntries();

        // check that we are not beyond allowed number of entries to copy from source chunk
        if (thisNumOfEntries >= maxCapacity) {
            return srcEntryIdx;
        }
        // assuming that all chunks are bounded with same number of entries to hold
        assert srcEntryIdx <= getMaxItems();

        // set the next entry index (previous entry or head) from where we start to copy
        // if sortedThisEntryIndex is one (first entry to be occupied on this chunk)
        // we are exactly going to update the head (ei=0)
        if (thisNumOfEntries == 0) {
            entryOrderedSet.setHeadEntryIndex(thisNumOfEntries);
        } else {
            entryOrderedSet.setNextEntryIndex(thisNumOfEntries - 1, thisNumOfEntries);
        }

        // Here was the code that was trying to read entries from srcEntryIdx on the source chunk
        // to see how much of them are subject for a copy, ordered and not deleted,
        // so theoretically they can be copied with copy array. The code is removed, because anyway
        // the long copy array doesn't happen since "next" needs to be updated separately.

        // copy entry by entry traversing the source linked list
        int curEntryIdx = srcEntryIdx;
        while (entryOrderedSet.copyEntry(tempValue, srcOrderedChunk.entryOrderedSet, curEntryIdx)) {
            // the source entry was either copied or disregarded as deleted
            // anyway move to next source entry (according to the linked list)
            curEntryIdx = srcOrderedChunk.entryOrderedSet.getNextEntryIndex(curEntryIdx);

            // if entry was ignored as deleted (no change in this EntryOrderedSet num of entries), continue
            if (thisNumOfEntries == entryOrderedSet.getNumOfEntries()) {
                continue;
            }

            // we indeed copied the entry, update the number of entries and the next pointer
            thisNumOfEntries++;
            entryOrderedSet.setNextEntryIndex(thisNumOfEntries - 1, thisNumOfEntries);

            // check that we are not beyond allowed number of entries to copy from source chunk
            if (thisNumOfEntries >= maxCapacity) {
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
        if (thisNumOfEntries == 0) {
            entryOrderedSet.setHeadEntryIndex(NONE_NEXT);
        } else {
            entryOrderedSet.setNextEntryIndex(thisNumOfEntries - 1, NONE_NEXT);
        }

        // sorted count keeps the number of sorted entries
        sortedCount.set(thisNumOfEntries);
        statistics.updateInitialCount(sortedCount.get());

        // check the validity of the new entryOrderedSet
        assert entryOrderedSet.isEntrySetValidAfterRebalance();

        return curEntryIdx; // if NONE_NEXT then we finished copying old chunk, else we reached max in new chunk
    }

    /**
     * marks this chunk's next pointer so this chunk is marked as deleted
     *
     * @return the next chunk pointed to once marked (will not change)
     */
    OrderedChunk<K, V> markAndGetNext() {
        // new chunks are ready, we mark frozen chunk's next pointer so it won't change
        // since next pointer can be changed by other split operations we need to do this in a loop - until we succeed
        while (true) {
            // if chunk is marked - that is ok and its next pointer will not be changed anymore
            // return whatever chunk is set as next
            if (next.isMarked()) {
                return next.getReference();
            } else { // otherwise try to mark it
                // read chunk's current next
                OrderedChunk<K, V> savedNext = next.getReference();

                // try to mark next while keeping the same next chunk - using CAS
                // if we succeeded then the next pointer we remembered is set and will not change - return it
                if (next.compareAndSet(savedNext, savedNext, false, true)) {
                    return savedNext;
                }
            }
        }
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

    /**
     * Descending iterator from the end of the chunk.
     * The lower bound given by parameter key "to" might not be in this chunk
     */
    DescendingIter descendingIter(ThreadContext ctx, K to, boolean toInclusive) {
        return new DescendingIter(ctx, to, toInclusive);
    }

    /**
     * Descending iterator from key "from" given as parameter.
     * The lower bound given by parameter key "to" might not be in this chunk
     */
    DescendingIter descendingIter(ThreadContext ctx, K from, boolean fromInclusive, K to,
        boolean toInclusive) {
        return new DescendingIter(ctx, from, fromInclusive, to, toInclusive);
    }

    /********************************************************************************************/
    /*------ Base Class for OrderedChunk Iterators (keeps all the common fields and methods) ----------*/

    // specifier whether the end boundary check needs to be performed on the current scan output
    enum IterEndBoundCheck {
        NEVER_END_BOUNDARY_CHECK,
        MID_END_BOUNDARY_CHECK,
        ALWAYS_END_BOUNDARY_CHECK
    }

    abstract class ChunkIter implements BasicChunkIter {
        protected int next;         // index of the next entry to be returned
        protected K endBound;       // stop bound key, or null if no stop bound
        protected boolean endBoundInclusive;  // inclusion flag for "to"

        protected IterEndBoundCheck isEndBoundCheckNeeded = IterEndBoundCheck.NEVER_END_BOUNDARY_CHECK;
        protected int midIdx = sortedCount.get() / 2; // approximately index of the middle key in the chunk


        boolean isBoundCheckNeeded() {
            return isEndBoundCheckNeeded == IterEndBoundCheck.ALWAYS_END_BOUNDARY_CHECK;
        };

        /** Checks if the given 'boundKey' key is beyond the scope of the given scan,
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

            if (midIdx != 0 && midIdx != -1) {
                // midIdx==0 if this is the initial chunk (sortedCount == 0) or midIdx is out of scope
                // midIdx==-1 if midIdx is out of the scope of this scan (lies before the start of the scan),
                // it is caught when traversing to the start bound and midIdx is set to -1

                // is the key in the middle index already above the upper limit to stop on?
                readKeyFromEntryIndex(ctx.tempKey, midIdx);
                if (!isKeyOutOfEndBound(ctx.tempKey)) {
                    isEndBoundCheckNeeded = IterEndBoundCheck.MID_END_BOUNDARY_CHECK;
                }
                // otherwise didn't succeed to delay the check
            }
        }
    }

    /********************************************************************************************/
    /*------------------------------- Iterators Implementations --------------------------------*/
    class AscendingIter extends ChunkIter {

        AscendingIter(ThreadContext ctx, K to, boolean toInclusive,
            OakScopedReadBuffer nextChunkMinKey) {
            next = entryOrderedSet.getHeadNextEntryIndex();
            next = advanceNextIndexNoBound(next, ctx);
            setIsEndBoundCheckNeeded(ctx, to, toInclusive, nextChunkMinKey);
        }

        AscendingIter(ThreadContext ctx, K from, boolean fromInclusive, K to, boolean toInclusive,
            OakScopedReadBuffer nextChunkMinKey) {
            KeyBuffer tempKeyBuff = ctx.tempKey;
            next = binaryFind(tempKeyBuff, from);

            if (next >= midIdx) { // binaryFind output is always less than sortedCount; or NONE_NEXT (0)
                midIdx = -1; // midIdx is not in the scope of this scan (too low)
            }
            // otherwise (next < midIdx) means that midIdx is surely of one of the entries to be scanned,
            // if not binaryFind will return midIdx or higher

            next = (next == NONE_NEXT) ?
                entryOrderedSet.getHeadNextEntryIndex() : entryOrderedSet.getNextEntryIndex(next);
            int compare = -1;
            if (next != NONE_NEXT) {
                compare = compareKeyAndEntryIndex(tempKeyBuff, from, next);
            }
            while (next != NONE_NEXT &&
                    (compare > 0 || (compare >= 0 && !fromInclusive) ||
                        !entryOrderedSet.isValueRefValidAndNotDeleted(next))) {
                next = entryOrderedSet.getNextEntryIndex(next);
                if (next != NONE_NEXT) {
                    compare = compareKeyAndEntryIndex(tempKeyBuff, from, next);
                }
            }
            // the setting of the stop bound check should know if midIdx is not in the scope of this scan
            // (too low); So setUpperBoundThreshold can be invoked only after 'next' is defined
            setIsEndBoundCheckNeeded(ctx, to, toInclusive, nextChunkMinKey);
        }

        private void advance(ThreadContext ctx) {
            next = entryOrderedSet.getNextEntryIndex(next);
            // if there is no need to check the end-boundary on this chunk (IterEndBoundCheck.NEVER_END_BOUNDARY_CHECK),
            // or if the caller will check the end-boundary (IterEndBoundCheck.ALWAYS_END_BOUNDARY_CHECK),
            // then advance next without additional checks
            if (isEndBoundCheckNeeded != IterEndBoundCheck.MID_END_BOUNDARY_CHECK) {
                next = advanceNextIndexNoBound(next, ctx);
            } else {
                next = advanceNextIndex(next, ctx);
            }
        }

        @Override
        public boolean hasNext() {
            return next != NONE_NEXT;
        }

        @Override
        public int next(ThreadContext ctx) {
            int toReturn = next;
            advance(ctx);
            return toReturn;
        }

        private int advanceNextIndex(final int entryIndex, ThreadContext ctx) {
            int next = entryIndex;
            while (next != NONE_NEXT && !entryOrderedSet.isValueRefValidAndNotDeleted(next)) {
                next = entryOrderedSet.getNextEntryIndex(next);
                if (isEndBoundCheckNeeded == IterEndBoundCheck.MID_END_BOUNDARY_CHECK && next == midIdx) {
                    // update isEndBoundCheckNeeded to ALWAYS_END_BOUNDARY_CHECK
                    // when reaching the midIndex
                    isEndBoundCheckNeeded = IterEndBoundCheck.ALWAYS_END_BOUNDARY_CHECK;
                }
            }
            return next;
        }

        private int advanceNextIndexNoBound(final int entryIndex, ThreadContext ctx) {
            int next = entryIndex;
            while (next != NONE_NEXT && !entryOrderedSet.isValueRefValidAndNotDeleted(next)) {
                next = entryOrderedSet.getNextEntryIndex(next);
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
            int c = KeyUtils.compareEntryKeyAndSerializedKey(endBound, (KeyBuffer) key, config.comparator);
            // return true if endBound<key or endBound==key and the scan was not endBoundInclusive
            return c < 0 || (c == 0 && !endBoundInclusive);
        }
    }

    class DescendingIter extends ChunkIter {
        private int anchor;
        private int prevAnchor;
        private final IntStack stack;
        private final K from;
        private boolean fromInclusive;
        private final int skipEntriesForBiggerStack = Math.max(1, getMaxItems() / 10); // 1 is the lowest possible value

        DescendingIter(ThreadContext ctx, K to, boolean toInclusive) {
            KeyBuffer tempKeyBuff = ctx.tempKey;
            setIsEndBoundCheckNeeded(ctx, to, toInclusive, minKey);
            from = null;
            stack = new IntStack(entryOrderedSet.getLastEntryIndex() + 1);
            int sortedCnt = sortedCount.get();
            anchor = // this is the last sorted entry
                    (sortedCnt == 0 ? entryOrderedSet.getHeadNextEntryIndex() : sortedCnt - 1);
            stack.push(anchor);
            initNext(tempKeyBuff);
        }

        DescendingIter(ThreadContext ctx, K from, boolean fromInclusive, K to, boolean toInclusive) {
            KeyBuffer tempKeyBuff = ctx.tempKey;

            this.from = from;
            this.fromInclusive = fromInclusive;
            stack = new IntStack(entryOrderedSet.getLastEntryIndex());
            anchor = binaryFind(tempKeyBuff, from);

            if (anchor <= midIdx) { // binaryFind output is always less than sorted Count; or NONE_NEXT
                midIdx = -1; // midIdx is not in the scope of this scan (too high)
            }
            // otherwise (next > midIdx) means that midIdx is surely of one of the entries to be scanned,
            // if not binaryFind will return midIdx or less

            // translate to be valid index, if anchor is head we know to stop the iteration
            anchor = (anchor == NONE_NEXT) ? entryOrderedSet.getHeadNextEntryIndex() : anchor;
            stack.push(anchor);
            initNext(tempKeyBuff);
            setIsEndBoundCheckNeeded(ctx, to, toInclusive, minKey);
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
            while (next != NONE_NEXT && !entryOrderedSet.isValueRefValidAndNotDeleted(next)) {
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
                    next = entryOrderedSet.getNextEntryIndex(next);
                } else {
                    if (next != prevAnchor) {
                        stack.push(next);
                        next = entryOrderedSet.getNextEntryIndex(next);
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
            assert stack.size() == 1;   // anchor is in the stack
            if (prevAnchor == entryOrderedSet.getNextEntryIndex(anchor)) {
                next = NONE_NEXT;   // there is no next;
                return;
            }
            next = entryOrderedSet.getNextEntryIndex(anchor);
            if (from == null) {
                // if this is not the first invocation, stop when reaching previous anchor
                pushToStack(!firstTimeInvocation);
            } else {
                if (firstTimeInvocation) {
                    final int threshold = fromInclusive ? -1 : 0;
                    // This is equivalent to continue while:
                    //         when fromInclusive: CMP >= 0
                    //     when non-fromInclusive: CMP > 0
                    while (next != NONE_NEXT && compareKeyAndEntryIndex(tempKeyBuff, from, next) > threshold) {
                        stack.push(next);
                        next = entryOrderedSet.getNextEntryIndex(next);
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
            if (anchor == entryOrderedSet.getHeadNextEntryIndex()) {
                next = NONE_NEXT; // there is no more in this chunk
                return;
            } else if (anchor == 1) { // cannot get below the first index
                anchor = entryOrderedSet.getHeadNextEntryIndex();
            } else {
                if ((anchor - skipEntriesForBiggerStack) > 1) {
                    // try to skip more then one backward step at a time
                    // if it shows better performance
                    anchor -= skipEntriesForBiggerStack;
                    // when bypassing the midIdx we miss some opportunities to avoid boundary check
                } else {
                    anchor -= 1;
                }
            }
            // midIdx can be only one of the anchors
            if (isEndBoundCheckNeeded == IterEndBoundCheck.MID_END_BOUNDARY_CHECK && anchor <= midIdx) {
                // update isEndBoundCheckNeeded to ALWAYS_END_BOUNDARY_CHECK
                // when reaching the midIndex as an anchor
                isEndBoundCheckNeeded = IterEndBoundCheck.ALWAYS_END_BOUNDARY_CHECK;
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
                if (anchor == entryOrderedSet.getHeadNextEntryIndex()) {
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

        @Override
        protected boolean isKeyOutOfEndBound(OakScopedReadBuffer key) {
            if (endBound == null) {
                return false;
            }
            int c = KeyUtils.compareEntryKeyAndSerializedKey(endBound, (KeyBuffer) key, config.comparator);
            // return true if endBound>key or if endBound==key and the scan was not endBoundInclusive
            return c > 0 || (c == 0 && !endBoundInclusive);
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
}
