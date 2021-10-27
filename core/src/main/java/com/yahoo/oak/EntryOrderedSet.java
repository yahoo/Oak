/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.ValueUtils.ValueResult;

import java.util.concurrent.atomic.AtomicInteger;

/* EntryOrderedSet keeps a set of entries linked to a link list. Entry is reference to key and value, both located
 * off-heap. EntryOrderedSet provides access, updates and manipulation on each entry, provided its index.
 *
 * Entry is a set of at least 2 fields (consecutive longs), part of "entries" int array. The definition
 * of each long is explained below. Also bits of each long (references) are represented in a very special
 * way (also explained below). The bits manipulations are ReferenceCodec responsibility.
 * Please update with care.
 *
 * Entries Array:
 * --------------------------------------------------------------------------------------
 * 0 | Key Reference          | Reference encoding all info needed to           |
 *   |                        | access the key off-heap                         | entry with
 * -----------------------------------------------------------------------------| entry index
 * 1 | Value Reference        | Reference encoding all info needed to           | 0
 *   |                        | access the key off-heap.                        |
 *   |                        | The encoding of key and value can be different  | entry that
 * -----------------------------------------------------------------------------| was allocated
 * 2 | NEXT  - entry index of the entry following the entry with entry index 0  | first
 * ---------------------------------------------------------------------------------------
 * 3 | Key Reference          | Reference encoding all info needed to           |
 *   |                        | access the key off-heap                         |
 * -----------------------------------------------------------------------------| entry with
 * 4 | Value Reference        | Reference encoding all info needed to           | entry index
 *   |                        | access the key off-heap.                        | 1
 * -----------------------------------------------------------------------------|
 * 5 | NEXT  - entry index of the entry following the entry with entry index 1  |
 * ---------------------------------------------------------------------------------------
 * 6 | Key Reference          | Reference encoding all info needed to           |
 *   |                        | access the key off-heap                         |
 * -----------------------------------------------------------------------------| entry with
 * 7 | Value Reference        | Reference encoding all info needed to           | entry index
 *   |                        | access the key off-heap.                        | 2
 * -----------------------------------------------------------------------------|
 * 8 | NEXT  - entry index of the entry following the entry with entry index 2  |
 * ---------------------------------------------------------------------------------------
 * ...
 *
 *
 * Internal class, package visibility
 */
class EntryOrderedSet<K, V> extends EntryArray<K, V> {

    /*-------------- Constants --------------*/

    /***
     * NEXT - the next index of this entry (one integer). Must be with offset 0, otherwise, copying an entire
     * entry should be fixed (In function {@code copyPartOfEntries}, search for "LABEL").
     */
    private static final int NEXT_FIELD_OFFSET = 2;

    // the size of the head in longs
    // how much it takes to keep the index of the first item in the list, after the head
    // (not necessarily first in the array!)
    private static final int ADDITIONAL_FIELDS = 1;  // # of primitive fields in each item of entries array

    // location of the first (head) node
    private AtomicInteger headEntryIndex = new AtomicInteger(INVALID_ENTRY_INDEX);

    // points to next free index of entry array, counted in "entries" and not in integers
    private final AtomicInteger nextFreeIndex;

    /*----------------- Constructor -------------------*/

    /**
     * Create a new EntryOrderedSet
     * @param vMM   for values off-heap allocations and releases
     * @param kMM off-heap allocations and releases for keys
     * @param entriesCapacity how many entries should this EntryOrderedSet keep at maximum
     * @param keySerializer   used to serialize the key when written to off-heap
     */
    EntryOrderedSet(MemoryManager vMM, MemoryManager kMM, int entriesCapacity, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer) {
        super(vMM, kMM, ADDITIONAL_FIELDS, entriesCapacity, keySerializer, valueSerializer);
        this.nextFreeIndex = new AtomicInteger( 0);
    }

    int getLastEntryIndex() {
        return nextFreeIndex.get();
    }

    /********************************************************************************************/
    /*------------- Methods for managing next entry indexes (package visibility) ---------------*/

    /**
     * getNextEntryIndex returns the next entry index (of the entry given by entry index "ei")
     * The method serves external EntryOrderedSet users.
     */
    int getNextEntryIndex(int ei) {
        if (!isIndexInBound(ei)) {
            return INVALID_ENTRY_INDEX;
        }
        return (int) getEntryFieldLong(ei, NEXT_FIELD_OFFSET);
    }

    /**
     * setNextEntryIndex sets the next entry index (of the entry given by entry index "ei")
     * to be the "next". Input parameter "next" must be a valid entry index (not integer index!).
     * The method serves external EntryOrderedSet users.
     */
    void setNextEntryIndex(int ei, int next) {
        assert ei <= nextFreeIndex.get() && next <= nextFreeIndex.get();
        setEntryFieldLong(ei, NEXT_FIELD_OFFSET, next);
    }

    /**
     * Set the index of the first entry in the linked list,
     * when there is no concurrency (i.e. rebalance)
     * @param headEntryIndex
     */
    void setHeadEntryIndex(int headEntryIndex) {
        this.headEntryIndex.set(headEntryIndex);
    }

    /**
     * casNextEntryIndex CAS the next entry index (of the entry given by entry index "ei") to be the
     * "nextNew" only if it was "nextOld". Input parameter "nextNew" must be a valid entry index.
     * The method serves external EntryOrderedSet users.
     */
    boolean casNextEntryIndex(int ei, int nextOld, int nextNew) {
        return casEntryFieldLong(ei, NEXT_FIELD_OFFSET, nextOld, nextNew);
    }

    /**
     * CAS the index of the first entry in the linked list, when concurrency can cause other
     * concurrent trials to update the same field
     * @param nextOld
     * @param nextNew
     * @return
     */
    boolean casHeadEntryIndex(int nextOld, int nextNew) {
        return this.headEntryIndex.compareAndSet(nextOld, nextNew);
    }


    /**
     * getHeadEntryIndex returns the entry index of the entry first in the array,
     * which is written in the first integer of the array
     * The method serves external EntryOrderedSet users.
     */
    int getHeadNextEntryIndex() {
        return headEntryIndex.get();
    }


    /********************************************************************************************/
    /*--------- Methods for managing the write/remove path of the keys and values  -------------*/

    /**
     * Creates/allocates an entry for the key. An entry is always associated with a key,
     * therefore the key is written to off-heap and associated with the entry simultaneously.
     * The value of the new entry is set to NULL: (INVALID_VALUE_REFERENCE)
     *
     * @param ctx the context that will follow the operation following this key allocation
     * @param key the key to write
     * @return true only if the allocation was successful.
     *         Otherwise, it means that the EntryOrderedSet is full (may require a re-balance).
     **/
    boolean allocateEntryAndWriteKey(ThreadContext ctx, K key) {
        ctx.invalidate();

        int ei = nextFreeIndex.getAndIncrement();

        if (!isIndexInBound(ei)) {
            return false;
        }
        numOfEntries.getAndIncrement();
        ctx.entryIndex = ei;
        // Write given key object "key" (to off-heap) as a serialized key, referenced by entry
        // that was set in this context ({@code ctx}).
        writeKey(key, ctx.key);
        // The entry key reference is set. The value reference is already set to zero, because
        // the entries array is initialized that way (see specs).
        setKeyReference(ctx.entryIndex, ctx.key.getSlice().getReference());

        return true;
    }

    /**
     * deleteValueFinish completes the deletion of a value in Oak, by marking the value reference in
     * entry, after the on-heap value was already marked as deleted.
     *
     * deleteValueFinish is used to CAS the value reference to it's deleted mode
     *
     * @param ctx The context that follows the operation since the key was found/created.
     *            Holds the entry to change, the old value reference to CAS out, and the current value reference.
     * @return true if the deletion indeed updated the entry to be deleted as a unique operation
     * <p>
     * Note 1: the value in {@code ctx} is updated in this method to be the DELETED.
     * <p>
     * Note 2: updating of the entries MUST be under published operation. The invoker of this method
     * is responsible to call it inside the publish/unpublish scope.
     */
    boolean deleteValueFinish(ThreadContext ctx) {
        if (valuesMemoryManager.isReferenceDeleted(ctx.value.getSlice().getReference())) {
            return false; // value reference in the slice is marked deleted
        }

        assert ctx.entryState == EntryState.DELETED_NOT_FINALIZED;

        // Value's reference codec prepares the reference to be used after value is deleted
        long expectedReference = ctx.value.getSlice().getReference();
        long newReference = valuesMemoryManager.alterReferenceForDelete(expectedReference);
        // Scenario:
        // 1. The value's slice is marked as deleted off-heap and the thread that started
        //    deleteValueFinish falls asleep.
        // 2. This value's slice space is allocated once again and assigned into the same entry.
        // 3. A valid value reference is CASed to invalid.
        //
        // This is ABA problem and resolved via always changing deleted variation of the reference
        // Also value's off-heap slice is released to memory manager only after deleteValueFinish
        // is done.
        if (casEntryFieldLong(ctx.entryIndex, VALUE_REF_OFFSET, expectedReference, newReference)) {
            assert valuesMemoryManager.isReferenceConsistent(getValueReference(ctx.entryIndex));
            numOfEntries.getAndDecrement();
            ctx.value.getSlice().release();
            ctx.value.invalidate();
            ctx.entryState = EntryState.DELETED;

            return true;
        }
        // CAS wasn't successful, someone else set the value reference as deleted and
        // maybe value was allocated again. Let the context's value to be updated
        readValue(ctx);
        return false;
    }

    /************************* REBALANCE *****************************************/
    /*
     * All the functionality that links entries into a linked list or updates the linked list
     * is provided by the user of the entry set. EntryOrderedSet provides the possibility to update the
     * next entry index via set or CAS, but does it only as a result of the user request.
     * */

    /**
     * copyEntry copies one entry from source EntryOrderedSet (at source entry index "srcEntryIdx")
     * to this EntryOrderedSet.
     * The destination entry index is chosen according to this nextFreeIndex which is increased with
     * each copy. Deleted entry (marked on-heap or off-heap) is not copied (disregarded).
     * <p>
     * The next pointers of the entries are requested to be set by the user if needed.
     *
     * @param tempValue   a reusable buffer object for internal temporary usage
     * @param srcEntryOrderedSet another EntryOrderedSet to copy from
     * @param srcEntryIdx the entry index to copy from {@code srcEntryOrderedSet}
     * @return false when this EntryOrderedSet is full
     * <p>
     * Note: NOT THREAD SAFE
     */
    boolean copyEntry(ValueBuffer tempValue, EntryOrderedSet<K, V> srcEntryOrderedSet, int srcEntryIdx) {
        if (srcEntryIdx == INVALID_ENTRY_INDEX) {
            return false;
        }

        assert srcEntryOrderedSet.isIndexInBound(srcEntryIdx);

        // don't increase the nextFreeIndex yet, as the source entry might not be copies
        int destEntryIndex = nextFreeIndex.get();

        if (!isIndexInBound(destEntryIndex)) {
            return false;
        }

        if (srcEntryOrderedSet.isValueDeleted(tempValue, srcEntryIdx)) {
            return true;
        }

        assert valuesMemoryManager.isReferenceConsistent(tempValue.getSlice().getReference());

        // ARRAY COPY: using next as the base of the entry
        // copy both the key and the value references and integer for future use => 5 integers via array copy
        // the first field in an entry is next, and it is not copied since it should be assigned elsewhere
        // therefore, to copy the rest of the entry we use the offset of next (which we assume is 0) and
        // add 1 to start the copying from the subsequent field of the entry.
        copyEntriesFrom(srcEntryOrderedSet, srcEntryIdx, destEntryIndex, 2);

        assert valuesMemoryManager.isReferenceConsistent(getValueReference(destEntryIndex));

        // now it is the time to increase nextFreeIndex
        nextFreeIndex.getAndIncrement();
        numOfEntries.getAndIncrement();
        return true;
    }

    boolean isEntrySetValidAfterRebalance() {
        int currIndex = getHeadNextEntryIndex();
        int prevIndex = INVALID_ENTRY_INDEX;
        while (currIndex > getLastEntryIndex()) {
            if (!isValueRefValidAndNotDeleted(currIndex)) {
                return false;
            }
            if (!valuesMemoryManager.isReferenceConsistent(getValueReference(currIndex))) {
                return false;
            }
            if (prevIndex != INVALID_ENTRY_INDEX && currIndex - prevIndex != 1) {
                return false;
            }
            prevIndex = currIndex;
            currIndex = getNextEntryIndex(currIndex);
        }

        return true;
    }
    
    void releaseAllDeletedKeys() {
        KeyBuffer key = new KeyBuffer(keysMemoryManager.getEmptySlice());
        for (int i = 0; i < getNumOfEntries(); i++) {
            if (valuesMemoryManager.isReferenceDeleted(getValueReference(i))) {
                if (keysMemoryManager.isReferenceValidAndNotDeleted(getKeyReference(i))) {
                    //TODO may add exception to isDelted and stop deleting if key is already deleted
                    key.s.decodeReference(getKeyReference(i));
                    if (key.s.isDeleted() != ValueResult.TRUE) {
                        if (key.s.logicalDelete() == ValueResult.TRUE) {
                            key.s.release();
                        }
                    }
                }
            }
        }
    }
}
