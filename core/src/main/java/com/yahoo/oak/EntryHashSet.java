/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicInteger;

/* EntryHashSet keeps a set of entries placed according to the hash function.
 * Entry is reference to key and value, both located off-heap.
 * EntryHashSet provides access, updates and manipulation on each entry, provided its entry hash index.
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
 * -----------------------------------------------------------------------------| entry hash index
 * 1 | Value Reference        | Reference encoding all info needed to           | 0
 *   |                        | access the key off-heap.                        |
 *   |                        | The encoding of key and value can be different  |
 * -----------------------------------------------------------------------------|
 * 2 | Hash  - FULL entry hash index, which might be different (bigger) than    |
 *   |         entry hash index                                                 |
 * ---------------------------------------------------------------------------------------
 * 3 | Key Reference          | Deleted reference (still) encoding all info     |
 *   |                        | needed to access the key off-heap               |
 * -----------------------------------------------------------------------------| Deleted entry with
 * 4 | Value Reference        | Deleted reference(still)  encoding all info     | entry hash index
 *   |                        | needed to access the key off-heap.              | 1
 * -----------------------------------------------------------------------------|
 * 5 | Hash  - FULL entry hash index, which might be different (bigger) than    |
 *   |         entry hash index                                                 |
 * ---------------------------------------------------------------------------------------
 * 6 | Key Reference          | Invalid key reference                           | empty entry
 * -----------------------------------------------------------------------------|
 * 7 | Value Reference        | Invalid value reference                         |
 * -----------------------------------------------------------------------------|
 * 8 | 000...00                                                                 |
 * ---------------------------------------------------------------------------------------
 * ...
 *
 *
 * Internal class, package visibility
 */
class EntryHashSet<K, V> extends EntryArray<K, V> {

    /*-------------- Constants --------------*/

    // HASH - the full hash index of this entry (one integer, all bits).
    private static final int HASH_FIELD_OFFSET = 2;

    // One additional to the key and value references field, which keeps the full hash index of the entry
    private static final int ADDITIONAL_FIELDS = 1;  // # of primitive fields in each item of entries array

    // number of entries candidates to try in case of collision
    private AtomicInteger collisionEscapes = new AtomicInteger(3);

    private final OakComparator<K> comparator;

    /*----------------- Constructor -------------------*/

    /**
     * Create a new EntryHashSet
     * @param vMM   for values off-heap allocations and releases
     * @param kMM off-heap allocations and releases for keys
     * @param entriesCapacity how many entries should this EntryOrderedSet keep at maximum
     * @param keySerializer   used to serialize the key when written to off-heap
     */
    EntryHashSet(MemoryManager vMM, MemoryManager kMM, int entriesCapacity, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer, OakComparator<K> comparator) {
        // We add additional field for the head (dummy) node
        super(vMM, kMM, ADDITIONAL_FIELDS, entriesCapacity, keySerializer, valueSerializer);
        this.comparator = comparator;
    }



    /********************************************************************************************/
    /*---------- Methods for managing full hash entry indexes (package visibility) -------------*/

    /**
     * getFullHashEntryIndex returns the full hash entry index (of the entry given by entry index "ei")
     * The method serves external EntryHashSet users.
     */
    int getFullHashEntryIndex(int ei) {
        if (!isIndexInBound(ei)) {
            return INVALID_ENTRY_INDEX;
        }
        return (int) getEntryFieldLong(ei, HASH_FIELD_OFFSET);
    }

    /**
     * setFullHashEntryIndex sets the full hash entry index (of the entry given by entry index "ei")
     * to be the "hi". Input parameter "hi" must be a valid hash entry index (in relevant bit size!).
     * The method serves external EntryHashSet users.
     */
    void setFullHashEntryIndex(int ei, int hi) {
        setEntryFieldLong(ei, HASH_FIELD_OFFSET, hi);
    }

    /**
     * casFullHashIndex CAS the full hash entry index (of the entry given by entry index "ei") to be the
     * "newHi" only if it was "oldHi". Input parameter "newHi" must be a valid entry index.
     * The method serves external EntryHashSet users.
     */
    boolean casFullHashEntryIndex(int ei, int oldHi, int newHi) {
        return casEntryFieldLong(ei, HASH_FIELD_OFFSET, oldHi, newHi);
    }

    /********************************************************************************************/
    /*----- Private Helpers -------*/

    /**
     * Compare a key with a serialized key that is pointed by a specific entry hash index,
     * and say whether they are equal.
     *
     * IMPORTANT:
     *  1. Assuming the entry's key is already read into the tempKeyBuff
     *
     * @param tempKeyBuff a reusable buffer object for internal temporary usage
     *                    As a side effect, this buffer will contain the compared
     *                    serialized key.
     * @param key         the key to compare
     * @param hi          the entry index to compare with
     * @return true - the keys are equal, false - otherwise
     */
    private boolean equalKeyAndEntryKey(KeyBuffer tempKeyBuff, K key, int hi, long fullKeyHashIdx) {
        // check the hash value comparision first
        if (getFullHashEntryIndex(hi) != fullKeyHashIdx) {
            return false;
        }
        return (0 == comparator.compareKeyAndSerializedKey(key, tempKeyBuff));
    }

    /* Check the entry in hashIdx for being occupied:
    * if key reference is invalid reference --> entry is vacant (EntryState.UNKNOWN)
    * if key is deleted (off-heap or reference):
    *   if value reference is deleted --> entry is fully deleted and thus vacant (EntryState.DELETED)
    *   otherwise              --> accomplish the full deletion process (if needed)
    *                                               (EntryState.DELETED_NOT_FINALIZED)
    * if value reference is valid --> entry is occupied (EntryState.VALID)
    * otherwise --> the entry is in process of being inserted
    *               if key is the same as ours --> we can participate in competition inserting value
    *               (EntryState.INSERT_NOT_FINALIZED)
    *               otherwise --> some else key is being inserted, consider entry being occupied
    *               (EntryState.VALID)
    */
    private EntryState getEntryState(ThreadContext ctx, int hi, K key, long fullKeyHashIdx) {
        if (getKeyReference(hi) == keysMemoryManager.getInvalidReference()) {
            return EntryState.UNKNOWN;
        }
        if (isKeyDeleted(ctx.tempKey, hi)) { // key is read to the ctx.tempKey as a side effect
            if (isValueRefValidAndNotDeleted(hi)) {
                return EntryState.DELETED_NOT_FINALIZED;
            } else {
                return EntryState.DELETED;
            }
        }
        // here the key is valid an not deleted, check for the value
        if (isValueRefValidAndNotDeleted(hi)) {
            return EntryState.VALID;
        }

        if (equalKeyAndEntryKey(ctx.tempKey, key, hi, fullKeyHashIdx)) {
            return EntryState.INSERT_NOT_FINALIZED;
        }
        return EntryState.VALID;
    }

    /**
     * findSuitableEntryForInsert finds the entry where given hey is going to be inserted.
     * Given initial hash index for the key, it checks entries[hashIdx] first and continues
     * to the next entries up to 'collisionEscapes', if the previous entries are all occupied.
     * If true is returned, ctx.entryIndex keeps the index of the chosen entry
     * and ctx.entryState keeps the state.
     *
     * IMPORTANT:
     *  1. Throws IllegalStateException if too much collisions are found
     *
     * @param ctx the context that will follow the operation following this key allocation
     * @param key the key to write
     * @param hashIdx hashIdx=fullHash||bit_size_mask
     * @param fullHashIdx fullHashIdx=hashFunction(key)
     * @return true only if the index is found (ctx.entryState keeps more details).
     *         Otherwise (false), re-balance is required
     *         ctx.entryIndex keeps the index of the chosen entry & ctx.entryState keeps the state
     */
    private boolean findSuitableEntryForInsert(ThreadContext ctx, K key, int hashIdx, long fullHashIdx) {
        // start from given hash index
        // and check the next `collisionEscapes` indexes if previous index is occupied
        int currentLocation = hashIdx;
        int collisionEscapesLocal = collisionEscapes.get();
        boolean newEntryFound = false;
        boolean sameKeyEntryFound = false;
        // as far as we didn't check more than `collisionEscapes` indexes
        while (Math.abs(currentLocation - hashIdx) < collisionEscapesLocal) {

            ctx.entryIndex = currentLocation; // check the entry candidate
            ctx.entryState = getEntryState(ctx, currentLocation, key, fullHashIdx);

            // EntryState.VALID --> entry is occupied, continue to next possible location
            // EntryState.DELETED_NOT_FINALIZED --> finish the deletion, then try to insert here
            // EntryState.UNKNOWN, EntryState.DELETED --> entry is vacant, try to insert the key here
            // EntryState.INSERT_NOT_FINALIZED --> you can compete to associate value with the same key
            switch (ctx.entryState) {
                case VALID:
                    currentLocation = (currentLocation + 1) % entriesCapacity; // cyclic increase
                    continue;
                case DELETED_NOT_FINALIZED:
                    deleteValueFinish(ctx); //TODO: deleteValueFinish needs to be updated
                    // deleteValueFinish() also changes the entry state to DELETED
                    // deliberate break through
                case DELETED: // deliberate break through
                case UNKNOWN:
                    newEntryFound = true;
                    break;
                case INSERT_NOT_FINALIZED:
                    sameKeyEntryFound = true; // continue without allocating a key
                    break;
                default:
                    throw new IllegalStateException("Unexpected Entry State");
            }
        }

        boolean differentFullHashIdxes = false;
        if (!newEntryFound && !sameKeyEntryFound) {
            // we checked allowed number of locations and all were occupied
            // if all occupied entries have same full hash index (rebalance won't help) --> increase collisionEscapes
            // otherwise --> rebalance
            currentLocation = hashIdx;
            while (collisionEscapesLocal > 1) {
                if (getFullHashEntryIndex(currentLocation) !=
                    getFullHashEntryIndex(currentLocation + 1)) {
                    differentFullHashIdxes = true;
                    break;
                }
                currentLocation++;
                collisionEscapesLocal--;
            }
            if (differentFullHashIdxes) {
                return false; // do rebalance
            } else {
                if (collisionEscapesLocal > 50) {
                    throw new IllegalStateException("Too much collisions for the hash function");
                }
                collisionEscapes.incrementAndGet();
                // restart recursively (hopefully won't happen too much)
                return findSuitableEntryForInsert(ctx, key, hashIdx, fullHashIdx);
            }
        }

        // entry state can only be DELETED or UNKNOWN or INSERT_NOT_FINALIZED
        assert (ctx.entryState == EntryState.DELETED || ctx.entryState == EntryState.UNKNOWN ||
            ctx.entryState == EntryState.INSERT_NOT_FINALIZED);
        return true;
    }

    /********************************************************************************************/
    /*------ Methods for managing the write/remove path of the hashed keys and values  ---------*/

    /**
     * Creates/allocates an entry for the key, given fullHashIdx=hashFunction(key)
     * and hashIdx=fullHash||bit_size_mask. An entry is always associated with a key,
     * therefore the key is written to off-heap and associated with the entry simultaneously.
     * The value of the new entry is set to NULL: (INVALID_VALUE_REFERENCE)
     *
     * @param ctx the context that will follow the operation following this key allocation
     * @param key the key to write
     * @param hashIdx hashIdx=fullHash||bit_size_mask
     * @param fullHashIdx fullHashIdx=hashFunction(key)
     * @return true only if the allocation was successful.
     *         Otherwise (false), rebalance is required
     **/
    boolean allocateKey(ThreadContext ctx, K key, int hashIdx, long fullHashIdx) {
        ctx.invalidate();
        if (!isIndexInBound(hashIdx)) {
            // cannot return "false" on illegal arguments,
            // it would just indicate unnecessary rebalnce requirement
            throw new IllegalArgumentException("Hash index out of bounds");
        }

        if (!findSuitableEntryForInsert(ctx, key, hashIdx, fullHashIdx)) {
            // rebalance is required
            return false;
        }

        if (ctx.entryState == EntryState.INSERT_NOT_FINALIZED) {
            // our key is already set in the relevant entry, but value is not yet set,
            // continue and compete on assigning the value
            return true;
        }

        // Here the chosen entry is either uninitialized, fully deleted
        // Write given key object "key" (to off-heap) as a serialized key, referenced by entry
        // that was set in this context ({@code ctx}).
        writeKey(key, ctx.key);
        // Try to assign our key
        if (casKeyReference(ctx.entryIndex, ctx.tempKey.getSlice().getReference(), /* old reference */
            ctx.key.getSlice().getReference() /* new reference */ )) {
            return true;
        }
        // CAS failed, does it failed because the same key as our was assigned?
        if (equalKeyAndEntryKey(ctx.tempKey, key, hashIdx, fullHashIdx)) {
            return true; // continue to compete on assigning the value
        }
        // CAS failed as other key was assigned restart and look for the entry again
        return allocateKey(ctx, key, hashIdx, fullHashIdx);
    }

    /**
     * writeValueCommit does the physical CAS of the value reference, which is the Linearization
     * Point of the insertion.
     *
     * @param ctx The context that follows the operation since the key was found/created.
     *            Holds the entry index to which the value reference is linked, the old and new
     *            value references.
     *
     * @return TRUE if the value reference was CASed successfully.
     */
    ValueUtils.ValueResult writeValueCommit(ThreadContext ctx) {
        // If the commit is for a writing the new value, the old values should be invalid.
        // Otherwise (commit is for moving the value) old value reference is saved in the context.

        long oldValueReference = ctx.value.getSlice().getReference();
        long newValueReference = ctx.newValue.getSlice().getReference();
        assert valuesMemoryManager.isReferenceValid(newValueReference);

        if (!casValueReference(ctx.entryIndex, oldValueReference, newValueReference)) {
            return ValueUtils.ValueResult.FALSE;
        }
        return ValueUtils.ValueResult.TRUE;
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

    /**
     * Checks if an entry is deleted (checks on-heap and off-heap).
     *
     * @param tempValue a reusable buffer object for internal temporary usage
     * @param ei        the entry index to check
     * @return true if the entry is deleted
     */
    boolean isEntryDeleted(ValueBuffer tempValue, int ei) {
        // checking the reference
        boolean isAllocatedAndNotDeleted = readValue(tempValue, ei);
        if (!isAllocatedAndNotDeleted) {
            return true;
        }
        // checking the off-heap data
        return tempValue.getSlice().isDeleted() != ValueUtils.ValueResult.FALSE;
    }


    /************************* REBALANCE *****************************************/
    /**/

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
    boolean copyEntry(ValueBuffer tempValue, EntryHashSet<K, V> srcEntryOrderedSet, int srcEntryIdx) {

        return true;
    }

    boolean isEntrySetValidAfterRebalance() {

        return true;
    }
}
