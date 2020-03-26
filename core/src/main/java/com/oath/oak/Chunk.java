/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.EmptyStackException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

import static com.oath.oak.ValueUtils.INVALID_VERSION;
import static com.oath.oak.ValueUtils.ValueResult.*;

public class Chunk<K, V> {
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

    private static final Unsafe unsafe = UnsafeUtils.unsafe;
    ByteBuffer minKey;       // minimal key that can be put in this chunk
    AtomicMarkableReference<Chunk<K, V>> next;
    OakComparator<K> comparator;

    // in split/compact process, represents parent of split (can be null!)
    private AtomicReference<Chunk<K, V>> creator;
    // chunk can be in the following states: normal, frozen or infant(has a creator)
    private final AtomicReference<State> state;
    private AtomicReference<Rebalancer<K, V>> rebalancer;
    private final EntrySet<K,V> entrySet;

    private AtomicInteger pendingOps;

    private final Statistics statistics;
    // # of sorted items at entry-array's beginning (resulting from split)
    private AtomicInteger sortedCount;
    private final int maxItems;
    AtomicInteger externalSize; // for updating oak's size (reference to one global per Oak size)

    /*-------------- Constructors --------------*/

    /**
     * Create a new chunk
     *
     * @param minKey  minimal key to be placed in chunk
     * @param creator the chunk that is responsible for this chunk creation
     */
    Chunk(ByteBuffer minKey, Chunk<K, V> creator, OakComparator<K> comparator, MemoryManager memoryManager,
          int maxItems, AtomicInteger externalSize, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
          ValueUtils valueOperator) {

        this.maxItems = maxItems;
        this.entrySet =
            new EntrySet<K,V>(memoryManager, maxItems, keySerializer, valueSerializer,
                valueOperator);
        // if not zero, sorted count keeps the entry index of the last
        // subsequent and ordered entry in the entries array
        this.sortedCount = new AtomicInteger(0);
        this.minKey = minKey;
        this.creator = new AtomicReference<>(creator);
        if (creator == null) {
            this.state = new AtomicReference<>(State.NORMAL);
        } else {
            this.state = new AtomicReference<>(State.INFANT);
        }
        this.next = new AtomicMarkableReference<>(null, false);
        this.pendingOps = new AtomicInteger();
        this.rebalancer = new AtomicReference<>(null); // to be updated on rebalance
        this.statistics = new Statistics();
        this.comparator = comparator;
        this.externalSize = externalSize;
    }

    /*-------------- Methods --------------*/

    void release() {
        // try to change the state
        state.compareAndSet(State.FROZEN, State.RELEASED);
    }

    ByteBuffer readMinKey() {
        return entrySet.readKey(entrySet.getHeadNextIndex());
    }

    ByteBuffer readMaxKey() {
        int maxEntry = getLastItemEntryIndex();
        return entrySet.readKey(maxEntry);
    }

    /**
     * look up key
     *
     * @param key the key to look up
     * @return a LookUp object describing the condition of the value associated with {@code key}.
     * If {@code lookup == null}, there is no entry with that key in this chunk.
     * If {@code lookup.valueReference == INVALID_VALUE}, it means that there is an entry with that key (can be
     * reused in case
     * of {@code put}, but there is no value attached to this key (it can happen if this entry is in the midst of
     * being inserted, or some thread removed this key and no rebalanace occurred leaving the entry in the chunk).
     * It implies that {@code lookup.valueSlice = null}.
     * If {@code lookup.valueSlice == null && lookup.valueReference != INVALID_VALUE} it means that the value is
     * marked off-heap as deleted, but the connection between the entry and the value was not unlinked yet.
     */
    EntrySet.LookUp lookUp(K key) {
        // binary search sorted part of key array to quickly find node to start search at
        // it finds previous-to-key
        int curr = binaryFind(key);
        curr = (curr == NONE_NEXT) ? entrySet.getHeadNextIndex() : entrySet.getNextEntryIndex(curr);
        int cmp;
        // iterate until end of list (or key is found)

        while (curr != NONE_NEXT) {
            // compare current item's key to searched key
            cmp = comparator.compareKeyAndSerializedKey(key, entrySet.readKey(curr));
            // if item's key is larger - we've exceeded our key
            // it's not in chunk - no need to search further
            if (cmp < 0) {
                return null;
            }
            // if keys are equal - we've found the item
            else if (cmp == 0) {
                return entrySet.buildLookUp(curr);
            }
            // otherwise- proceed to next item
            curr = entrySet.getNextEntryIndex(curr);
        }

        return null;
    }

    /**
     * This function completes the insertion (or deletion) of a value to Entry. When inserting a
     * value, the value reference is CASed inside the entry first and only afterwards the version is
     * CASed. Thus, there can be a time in which the value reference is valid but the version is
     * INVALID_VERSION or a negative one. In this function, the version is CASed to complete the
     * insertion.
     * <p>
     * The version written to entry is the version written in the off-heap memory. There is no worry
     * of concurrent removals since these removals will have to first call this function as well,
     * and they eventually change the version as well.
     *
     * @param lookUp - It holds the entry to CAS, the value version written in this entry and the
     *               value reference from which the correct version can be read.
     * @return a version is returned.
     * If it is {@code INVALID_VERSION} it means that a CAS was not preformed. Otherwise, a positive version is
     * returned, and it the version written to the entry (maybe by some other thread).
     * <p>
     * Note that the version in the input param {@code lookUp} is updated to be the right one if a valid version was
     * returned.
     */
    int completeLinking(EntrySet.LookUp lookUp) {
        if (!entrySet.isDeleteValueFinishNeeded(lookUp) && entrySet.isValueLinkFinished(lookUp)) {
            // the version written in lookup is a good one!
            return lookUp.version;
        }
        if (!publish()) {
            return INVALID_VERSION;
        }
        try {
            return entrySet.writeValueFinish(lookUp); // TODO: eliminate returning version
        } finally {
            unpublish();
        }
    }

    /**
     * As written in {@code writeValueFinish(LookUp)}, when changing an entry, the value reference is CASed first and
     * later the value version, and the same applies when removing a value. However, there is another step before
     * changing an entry to remove a value and it is marking the value off-heap (the LP). This function is used to
     * first CAS the value reference to {@code INVALID_VALUE_REFERENCE} and then CAS the version to be a negative one.
     * Other threads seeing a marked value call this function before they proceed (e.g., before performing a
     * successful {@code putIfAbsent}).
     *
     * @param lookUp - holds the entry to change, the old value reference to CAS out, and the current value version.
     * @return {@code true} if a rebalance is needed
     */
    boolean finalizeDeletion(EntrySet.LookUp lookUp) {

        if (!entrySet.isDeleteValueFinishNeeded(lookUp)) {
            return false;
        }
        if (!publish()) {
            return true;
        }
        try {
            if (!entrySet.deleteValueFinish(lookUp)) {
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
     * release key in slice, currently not in use, waiting for GC to be arranged
     *
     * @param lookUp*/
    void releaseKey(EntrySet.LookUp lookUp) {
        entrySet.releaseKey(lookUp);
    }

    // Use this function to release an unreachable value reference
    void releaseValue(EntrySet.OpData opData) {
        entrySet.releaseValue(opData);
    }

    // Check if value reference is valid. Doesn't check further than that
    // (meaning whether the underlined off-heap is marked deleted or version is negative)
    boolean isValueRefValid(int ei) {
      return entrySet.isValueRefValid(ei);
    }

    Slice buildValueSlice(int ei) {
      return entrySet.buildValueSlice(ei);
    }

    EntrySet.LookUp buildLookUp(int ei) {
        return entrySet.buildLookUp(ei);
    }

    ByteBuffer readKeyFromEntryIndex(int ei) {
        return entrySet.readKey(ei);
    }

    void setKeyReference(int ei, OakRReference keyRef) {
        entrySet.setKeyOutputRBuff(ei, keyRef);
    }

    boolean setValueReference(int ei, OakRReference valueRef) {
        return entrySet.setValueOutputRBuff(ei, valueRef);
    }

    /**
     * binary search for largest-entry smaller than 'key' in sorted part of key array.
     *
     * @return the index of the entry from which to start a linear search -
     * if key is found, its previous entry is returned!
     * In cases when search from the head is needed, meaning:
     * (1) the given key is less or equal than the smallest key in the chunk OR
     * (2) entries are unsorted so there is a need to start from the beginning of the linked list
     * NONE_NEXT is going to be returned
     */
    private int binaryFind(K key) {
        int sortedCount = this.sortedCount.get();
        int headIdx = entrySet.getHeadNextIndex();
        // if there are no sorted keys, or the first item is already larger than key -
        // return the head entry for a regular linear search
        if ((sortedCount == 0) ||
            comparator.compareKeyAndSerializedKey(key, entrySet.readKey(headIdx)) <= 0) {
            return NONE_NEXT;
        }

        // optimization: compare with last key to avoid binary search (here sortedCount is not zero)
        if (comparator.compareKeyAndSerializedKey(key, entrySet.readKey(sortedCount)) > 0) {
            return sortedCount;
        }

        int start = 0;
        int end = sortedCount;
        while (end - start > 1) {
            int curr = start + (end - start) / 2;
            if (comparator.compareKeyAndSerializedKey(key, entrySet.readKey(curr)) <= 0) {
                end = curr;
            } else {
                start = curr;
            }
        }

        return start;
    }

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

    EntrySet.LookUp allocateEntryAndKey(K key) {
        return entrySet.allocateEntry(key);
    }

    int linkEntry(EntrySet.LookUp lookUp, K key) {
        int prev, curr, cmp;
        int anchor = INVALID_ANCHOR_INDEX;
        int ei = lookUp.entryIndex;
        while (true) {
            // start iterating from quickly-found node (by binary search) in sorted part of order-array
            if (anchor == INVALID_ANCHOR_INDEX) {
                anchor = binaryFind(key);
            }
            if (anchor == NONE_NEXT) {
                prev = NONE_NEXT;
                curr = entrySet.getHeadNextIndex();
            } else {
                prev = anchor;
                curr = entrySet.getNextEntryIndex(anchor);    // index of next item in list
            }

            //TODO: use lookUp and location window inside lookUp (when key wasn't found),
            //TODO: so there us no need to iterate again in linkEntry
            // iterate items until key's position is found
            while (true) {
                // if no item, done searching - add to end of list
                if (curr == NONE_NEXT) {
                    break;
                }
                // compare current item's key to ours
                cmp = comparator.compareKeyAndSerializedKey(key, entrySet.readKey(curr));

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
            entrySet.setNextEntryIndex(ei,curr);
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
                        if (comparator.compareKeyAndSerializedKey(
                                key, entrySet.readKey(sortedCount)) >= 0) {
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
     * write value off-heap, promoted to EntrySet.
     *
     * @param value the value to write off-heap
     * @param writeForMove
     * @return OpData to be used later in the writeValueCommit
     **/
    EntrySet.OpData writeValue(EntrySet.LookUp lookUp, V value, boolean writeForMove) {
        return entrySet.writeValueStart(lookUp, value, writeForMove);
    }


    int getMaxItems() {
        return maxItems;
    }

    /**
     * This function does the physical CAS of the value reference, which is the LP of the insertion. It then tries to
     * complete the insertion (@see #writeValueFinish(LookUp)).
     * This is also the only place in which the size of Oak is updated.
     *
     * @param opData - holds the entry to which the value reference is linked, the old and new value references and
     *               the old and new value versions.
     * @param linkForMove
     * @param lookUp
     * @return {@code true} if the value reference was CASed successfully.
     */
    ValueUtils.ValueResult linkValue(EntrySet.OpData opData, boolean linkForMove,
        EntrySet.LookUp lookUp) {
        if (entrySet.writeValueCommit(opData, linkForMove) == FALSE) {
            return FALSE;
        }
        if (!linkForMove) {
            statistics.incrementAddedCount();
            externalSize.incrementAndGet();
        }
        return TRUE;
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

    Chunk<K, V> creator() {
        return creator.get();
    }

    State state() {
        return state.get();
    }

    private void setState(State state) {
        this.state.set(state);
    }

    void normalize() {
        state.compareAndSet(State.INFANT, State.NORMAL);
        creator.set(null);
        // using fence so other puts can continue working immediately on this chunk
        Chunk.unsafe.storeFence();
    }

    final int getFirstItemEntryIndex() {
        return entrySet.getHeadNextIndex();
    }

    private int getLastItemEntryIndex() {
        // find the last sorted entry
        int sortedCount = this.sortedCount.get();
        int entryIndex = sortedCount == 0 ? entrySet.getHeadNextIndex() : sortedCount;
        int nextEntryIndex = entrySet.getNextEntryIndex(entryIndex);
        while (nextEntryIndex != NONE_NEXT) {
            entryIndex = nextEntryIndex;
            nextEntryIndex = entrySet.getNextEntryIndex(entryIndex);
        }
        return entryIndex;
    }

    /**
     * freezes chunk so no more changes can be done to it (marks pending items as frozen)
     */
    void freeze() {
        setState(State.FROZEN); // prevent new puts to this chunk
        while (pendingOps.get() != 0) ;
    }

    /***
     * Copies entries from srcChunk (starting srcEntryIdx) to this chunk,
     * performing entries sorting on the fly (delete entries that are removed as well).
     * @param srcChunk -- chunk to copy from
     * @param srcEntryIdx -- start position for copying
     * @param maxCapacity -- max number of entries "this" chunk can contain after copy
     * @return entry index of next to the last copied entry (in the srcChunk),
     *              NONE_NEXT if all items were copied
     */
    final int copyPartNoKeys(Chunk<K, V> srcChunk, int srcEntryIdx, int maxCapacity) {

        if (srcEntryIdx == NONE_NEXT) {
            return NONE_NEXT;
        }

        // use local variables and just set the atomic variables once at the end
        int numOfEntries = entrySet.getNumOfEntries();
        // next *free* index of this entries array
        int sortedThisEntryIndex = numOfEntries+1;

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
        while(entrySet.copyEntry(srcChunk.entrySet, srcEntryIdx)) {
            // the source entry was either copied or disregarded as deleted
            // anyway move to next source entry (according to the linked list)
            srcEntryIdx = srcChunk.entrySet.getNextEntryIndex(srcEntryIdx);

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
            if (srcEntryIdx == NONE_NEXT) {
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
        return srcEntryIdx; // if NONE_NEXT then we finished copying old chunk, else we reached max in new chunk
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
            }
            // otherwise try to mark it
            else {
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
        return (sortedCount == 0 && numOfEntries * MAX_ENTRIES_FACTOR > maxItems) ||
                (sortedCount > 0 && (sortedCount * SORTED_REBALANCE_RATIO) < numOfEntries) ||
                (numOfEntries * MAX_IDLE_ENTRIES_FACTOR > maxItems && numOfItems * MAX_IDLE_ENTRIES_FACTOR < numOfEntries);
    }

    /*-------------- Iterators --------------*/

    AscendingIter ascendingIter() {
        return new AscendingIter();
    }

    AscendingIter ascendingIter(K from, boolean inclusive) {
        return new AscendingIter(from, inclusive);
    }

    DescendingIter descendingIter() {
        return new DescendingIter();
    }

    DescendingIter descendingIter(K from, boolean inclusive) {
        return new DescendingIter(from, inclusive);
    }

    private int advanceNextIndex(int next) {
        while (next != NONE_NEXT && !entrySet.isValueRefValid(next)) {
            next = entrySet.getNextEntryIndex(next);
        }
        return next;
    }

    interface ChunkIter {
        boolean hasNext();
        int next();
    }

    class AscendingIter implements ChunkIter {

        private int next;

        AscendingIter() {
            next = entrySet.getHeadNextIndex();
            next = advanceNextIndex(next);
        }

        AscendingIter(K from, boolean inclusive) {
            next = binaryFind(from);
            next = (next == NONE_NEXT) ? entrySet.getHeadNextIndex() : entrySet.getNextEntryIndex(next);
            int compare = -1;
            if (next != NONE_NEXT) {
                compare = comparator.compareKeyAndSerializedKey(from, entrySet.readKey(next));
            }
            while (next != NONE_NEXT &&
                (compare > 0 || (compare >= 0 && !inclusive) || !entrySet.isValueRefValid(next))) {
                next = entrySet.getNextEntryIndex(next);
                if (next != NONE_NEXT) {
                    compare = comparator.compareKeyAndSerializedKey(from, entrySet.readKey(next));
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
        public int next() {
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

        static final int SKIP_ENTRIES_FOR_BIGGER_STACK = 1; // 1 is the lowest possible value

        DescendingIter() {
            from = null;
            stack = new IntStack(entrySet.getLastEntryIndex());
            int sortedCnt = sortedCount.get();
            anchor = // this is the last sorted entry
                (sortedCnt == 0 ? entrySet.getHeadNextIndex() : sortedCnt);
            stack.push(anchor);
            initNext();
        }

        DescendingIter(K from, boolean inclusive) {
            this.from = from;
            this.inclusive = inclusive;
            stack = new IntStack(entrySet.getNumOfEntries());
            anchor = binaryFind(from);
            // translate to be valid index, if anchor is head we know to stop the iteration
            anchor = (anchor == NONE_NEXT) ? entrySet.getHeadNextIndex() : anchor;
            stack.push(anchor);
            initNext();
        }

        private void initNext() {
            traverseLinkedList(true);
            advance();
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
            while (next != NONE_NEXT && !entrySet.isValueRefValid(next)) {
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
         * @param firstTimeInvocation
         */
        private void traverseLinkedList(boolean firstTimeInvocation) {
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
                    if (inclusive) {
                        while (next != NONE_NEXT
                            && comparator.compareKeyAndSerializedKey(from, entrySet.readKey(next)) >= 0) {
                            stack.push(next);
                            next = entrySet.getNextEntryIndex(next);
                        }
                    } else {
                        while (next != NONE_NEXT
                            && comparator.compareKeyAndSerializedKey(from, entrySet.readKey(next)) > 0) {
                            stack.push(next);
                            next = entrySet.getNextEntryIndex(next);
                        }
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
                if ((anchor - SKIP_ENTRIES_FOR_BIGGER_STACK) > 1) {
                    // try to skip more then one backward step at a time
                    // if it shows better performance
                    anchor -= SKIP_ENTRIES_FOR_BIGGER_STACK;
                } else {
                    anchor -= 1;
                }
            }
            stack.push(anchor);
        }

        private void advance() {
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
                traverseLinkedList(false);
            }
        }

        @Override
        public boolean hasNext() {
            return next != NONE_NEXT;
        }

        @Override
        public int next() {
            int toReturn = next;
            advance();
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
            if (top == stack.length) {
                throw new ArrayIndexOutOfBoundsException();
            }
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
