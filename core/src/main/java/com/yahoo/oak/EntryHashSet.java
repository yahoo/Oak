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
    static final int INVALID_FULL_HASH = 0; // because memory is initially zeroed
    static final int DEFAULT_COLLISION_CHAIN_LENGTH = 3;
    // HASH - the full hash index of this entry (one integer, all bits).
    private static final int HASH_FIELD_OFFSET = 2;

    // One additional to the key and value references field, which keeps the full hash index of the entry
    private static final int ADDITIONAL_FIELDS = 1;  // # of primitive fields in each item of entries array

    // number of entries candidates to try in case of collision
    private AtomicInteger collisionChainLength = new AtomicInteger(DEFAULT_COLLISION_CHAIN_LENGTH);

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
     * getFullHashNumber returns the full hash entry index (the number only!) of the entry
     * given by entry index "ei". The method serves internal and external EntryHashSet users.
     */
    private int getFullHashNumber(int ei) {
        assert isIndexInBound(ei);
        // TODO: need to extract the number from the updates counter and to return only the integer
        return (int) getEntryFieldLong(ei, HASH_FIELD_OFFSET);
    }

    /**
     * getFullHashEntryField returns the full hash entry index (the entire field including counter!)
     * of the entry given by entry index "ei". The method serves internal and external EntryHashSet users.
     */
    private int getFullHashEntryField(int ei) {
        assert isIndexInBound(ei);
        return (int) getEntryFieldLong(ei, HASH_FIELD_OFFSET);
    }

    /**
     * isFullHashIndexValid checks the index itself disregarding the update counter
     */
    private boolean isFullHashIndexValid(int ei) {
        return (getFullHashNumber(ei) != INVALID_FULL_HASH);
    }

    /**
     * setFullHashEntryIndex sets the full hash entry index (of the entry given by entry index "ei")
     * to be the "fullHash".
     */
    private void setFullHashEntryIndex(int ei, long fullHash) {
        setEntryFieldLong(ei, HASH_FIELD_OFFSET, fullHash);
    }

    /**
     * casFullHashIndex CAS the full hash entry index (of the entry given by entry index "ei") to be the
     * "newFullHash" only if it was "oldFullHash"
     */
    private boolean casFullHashEntryIndex(int ei, long oldFullHash, long newFullHash) {
        //TODO: need to extract here the updates counter from the old hash
        //TODO: and to increase it and to add to the new hash
        return casEntryFieldLong(ei, HASH_FIELD_OFFSET, oldFullHash, newFullHash);
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
        int entryFullHash = getFullHashNumber(hi);
        if (entryFullHash != INVALID_FULL_HASH && entryFullHash != fullKeyHashIdx) {
            return false;
        }
        return (0 == comparator.compareKeyAndSerializedKey(key, tempKeyBuff));
    }

    @VisibleForTesting
    int getCollisionChainLength() {
        return collisionChainLength.get();
    }

    /* Check the entry in `hi` for being occupied
    ** Assume upon invocation that ctx.value ctx.key ctx.fullHash are invalidated
    ** At the end:
    ** If output is  EntryState.UNKNOWN --> ctx.key, ctx.value, ctx.fullHash remain untouched
    ** If output is  EntryState.DELETED_NOT_FINALIZED/DELETED/INSERT_NOT_FINALIZED/VALID
    ** --> ctx.key, ctx.value, ctx.fullHash keep the data of the entry's key, value, and full hash number
     */
    private EntryState getEntryState(ThreadContext ctx, int hi, K key, long fullKeyHashIdx) {

        if (getKeyReference(hi) == keysMemoryManager.getInvalidReference()) {
            return EntryState.UNKNOWN;
        }
        ctx.fullHash = getFullHashEntryField(hi);
        // entry was used already, is value deleted?
        // the linearization point of deletion is marking the value off-heap
        // isValueDeleted checks the reference first (for being invalid or deleted) than the off-heap header
        if (isValueDeleted(ctx.value, hi)) { // value is read to the ctx.value as a side effect
            // for later progressing with deleted entry read current key slice
            // (value is read during deleted key check, unless deleted)
            if (readKey(ctx.key, hi)) {
                // key is not deleted: either this deletion is not yet finished,
                // or this is a new assignment on top of deleted entry
                // Check the full hash index:
                // if invalid --> this is INSERT_NOT_FINALIZED if valid --> DELETED_NOT_FINALIZED
                if (isFullHashIndexValid(hi)) {
                    return EntryState.DELETED_NOT_FINALIZED;
                }
            } else {
                // key is deleted, check that full hash index is invalidated,
                // because it is the last stage of deletion
                return isFullHashIndexValid(hi) ? EntryState.DELETED_NOT_FINALIZED : EntryState.DELETED;
            }
            // not finalized insert is progressing out of this if
        }

        // read current key slice (value is read during delete check)
        readKey(ctx.key, ctx.entryIndex);

        // value is either invalid or valid (but not deleted), can be in progress of being inserted
        if (isValueRefValidAndNotDeleted(hi)) {
            return EntryState.VALID;
        }

        if (equalKeyAndEntryKey(ctx.key, key, hi, fullKeyHashIdx)) {
            if (!valuesMemoryManager.isReferenceValid(ctx.value.getSlice().getReference())) {
                return EntryState.INSERT_NOT_FINALIZED;
            }
        }
        return EntryState.VALID;
    }

    /**
     * lookUp checks whether key exists in the given hashIdx or after.
     * Given initial hash index for the key, it checks entries[hashIdx] first and continues
     * to the next entries up to 'collisionChainLength', if key wasn't previously found.
     * If true is returned, ctx.entryIndex keeps the index of the found entry
     * and ctx.entryState keeps the state.
     *
     * @param ctx the context that will follow the operation following this key allocation
     * @param key the key to write
     * @param hashIdx hashIdx=fullHash||bit_size_mask
     * @param fullHashIdx fullHashIdx=hashFunction(key)
     * @return true only if the key is found (ctx.entryState keeps more details).
     *         Otherwise (false), key wasn't found, ctx with it's buffers is invalidated
     */
    boolean lookUp(ThreadContext ctx, K key, int hashIdx, long fullHashIdx) {
        // start from given hash index
        // and check the next `collisionChainLength` indexes if previous index is occupied
        int collisionChainLengthLocal = collisionChainLength.get();

        // as far as we didn't check more than `collisionChainLength` indexes
        for (int i = 0; i < collisionChainLengthLocal; i++) {
            ctx.entryIndex = (hashIdx + i) % entriesCapacity; // check the entry candidate, cyclic increase
            // entry's key is read into ctx.tempKey as a side effect
            ctx.entryState = getEntryState(ctx, ctx.entryIndex, key, fullHashIdx);

            // value and key slices are read during getEntryState() unless the entry is
            // fully deleted (EntryState.DELETED) in this case we cannot compare the key (!)
            // also deletion linearization point is checked during getEntryState()
            if (ctx.entryState != EntryState.DELETED &&
                equalKeyAndEntryKey(ctx.key, key, ctx.entryIndex, fullHashIdx)) {
                // EntryState.VALID --> the key is found
                // DELETED_NOT_FINALIZED/UNKNOWN --> key doesn't exists
                //                      and there is no need to continue to check next entries
                // INSERT_NOT_FINALIZED --> before linearization point, key doesn't exist
                // when more than unique keys can be concurrently inserted, need to check further!
                return ctx.entryState == EntryState.VALID;
            }
            // not in this entry, move to next
        }
        ctx.invalidate();
        return false;
    }


    /**
     * findSuitableEntryForInsert finds the entry where given hey is going to be inserted.
     * Given initial hash index for the key, it checks entries[hashIdx] first and continues
     * to the next entries up to 'collisionChainLength', if the previous entries are all occupied.
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
        // and check the next `collisionChainLength` indexes if previous index is occupied
        int collisionChainLengthLocal = collisionChainLength.get();
        boolean entryFound = false;

        // as far as we didn't check more than `collisionChainLength` indexes
        for (int i = 0; i < collisionChainLengthLocal; i++) {
            ctx.invalidate();
            ctx.entryIndex = (hashIdx + i) % entriesCapacity; // check the entry candidate, cyclic increase
            // entry's key is read into ctx.tempKey as a side effect
            ctx.entryState = getEntryState(ctx, ctx.entryIndex, key, fullHashIdx);

            // EntryState.VALID --> entry is occupied, continue to next possible location
            // EntryState.DELETED_NOT_FINALIZED --> finish the deletion, then try to insert here
            // EntryState.UNKNOWN, EntryState.DELETED --> entry is vacant, try to insert the key here
            // EntryState.INSERT_NOT_FINALIZED --> you can compete to associate value with the same key
            switch (ctx.entryState) {
                case VALID:
                    if (equalKeyAndEntryKey(ctx.key, key, hashIdx, fullHashIdx)) {
                        // found valid entry has our key, the inserted key must be unique
                        // the entry state will indicate that insert didn't happen
                        return true;
                    }
                    continue;
                case DELETED_NOT_FINALIZED:
                    deleteValueFinish(ctx); //TODO: invocation must be within chunk publishing scope!
                    // deleteValueFinish() also changes the entry state to DELETED
                    // deliberate break through
                case DELETED: // deliberate break through
                case UNKNOWN:
                    // deliberate break through
                case INSERT_NOT_FINALIZED: // continue allocating value without allocating a key
                    entryFound = true;
                    break;
                default:
                    // For debugging the case new state is added and this code is not updated
                    assert false;
            }
            break;
        }

        if (!entryFound) {
            boolean differentFullHashIdxes = false;
            // we checked allowed number of locations and all were occupied
            // if all occupied entries have same full hash index (rebalance won't help)
            //           --> increase collisionChainLength
            // otherwise --> rebalance
            int currentLocation = hashIdx;
            while (collisionChainLengthLocal > 1) {
                if (getFullHashNumber(currentLocation) !=
                    getFullHashNumber(currentLocation + 1)) {
                    differentFullHashIdxes = true;
                    break;
                }
                currentLocation++;
                collisionChainLengthLocal--;
            }
            if (differentFullHashIdxes) {
                return false; // do rebalance
            } else {
                if (collisionChainLength.get() > (DEFAULT_COLLISION_CHAIN_LENGTH * 10)) {
                    throw new IllegalStateException("Too much collisions for the hash function");
                }
                collisionChainLength.incrementAndGet();
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
     * The value of the new entry is set to NULL (INVALID_VALUE_REFERENCE)
     * or can be some deleted value
     *
     * Upon successful finishing of allocateKey():
     * ctx.entryIndex keeps the index of the chosen entry
     * Reference of ctx.key is the reference pointing to the new key
     * Reference of ctx.value is either invalid or deleted reference to the previous entry's value
     *
     * The allocation won't happen for an existing key, although TRUE will be returned
     * (because no re-balance required). In this case the entry state should be EntryState.VALID.
     * The ctx.key will be populated to point to the found key
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

        if (ctx.entryState == EntryState.VALID || ctx.entryState == EntryState.INSERT_NOT_FINALIZED) {
            // our key exists, either in valid entry, or the key is already set in the relevant entry,
            // but value is not yet set. According to the entry
            // state either fail insertion or continue and compete on assigning the value

            //TODO: we need to check for key's full hash number being already set in the entry
            //TODO: if not need to apply hash function on the key and write it there
            //TODO: need to see how hash function can be applied
            return true;
        }

        // Here the chosen entry is either uninitialized or fully deleted.
        // Before writing a new key and to the ctx.key copy the ctx.key to ctx.tempKey
        ctx.tempKey.copyFrom(ctx.key);
        ctx.key.invalidate();

        // Write given key object "key" (to off-heap) as a serialized key, referenced by entry
        // that was set in this context ({@code ctx}).
        writeKey(key, ctx.key);

        // Try to assign our key
        if (casKeyReference(ctx.entryIndex, ctx.tempKey.getSlice().getReference(), /* old reference */
            ctx.key.getSlice().getReference() /* new reference */ )) {

            // key reference CASed (only one should succeed) write the entry's full hash,
            // because it is used for keys comparision (invalid full hash is not used for comparison)
            if ( casFullHashEntryIndex(ctx.entryIndex, ctx.fullHash, fullHashIdx) ) {
                return true;
            } else {
                // someone else proceeded with the same key if full hash is deleted we are totally late
                // check everything again
                return allocateKey(ctx, key, hashIdx, fullHashIdx);
            }
        }
        // CAS failed, does it failed because the same key as our was assigned?
        if (equalKeyAndEntryKey(ctx.tempKey, key, hashIdx, fullHashIdx)) {
            return true; // continue to compete on assigning the value
        }
        // CAS failed as other key was assigned restart and look for the entry again
        return allocateKey(ctx, key, hashIdx, fullHashIdx);

        // FOR NOW WE ASSUME NO SAME KEY IS INSERTED SIMULTANEOUSLY, SO CHECK IS OMITTED HERE
    }

    /**
     * deleteValueFinish completes the deletion of a mapping in OakHash.
     * The linearization point is (1) marking the delete bit in the value's off-heap header,
     * this must be done before invoking deleteValueFinish.
     *
     * The rest is: (2) marking the delete bit in the key's off-heap header;
     * (3) marking the value reference as deleted
     * (4) marking the key reference as deleted;
     * (5) invalidate the full hash idx as well
     *
     * All the actions are made via CAS and thus idempotent. All the expected CAS values are taken
     * from ctx that must be previously updated (when the entry was first found). The return value
     * is true if the delete help was actually needed, or false otherwise.
     *
     * @param ctx The context that follows the operation since the key was found/created.
     *            Holds the entry to change, the old value reference to CAS out, and the current value reference.
     * @return true if the deletion indeed updated the entry to be deleted as a unique operation
     * <p>
     * Note 1: the entry state in {@code ctx} is updated in this method to be the DELETED.
     * <p>
     *  Note 2: the invocation must be withing publishing scope to ensure marking the
     *  references as deleted is unique and so the slice releases
     */
    boolean deleteValueFinish(ThreadContext ctx) {
        if (valuesMemoryManager.isReferenceDeleted(ctx.value.getSlice().getReference())
            && getFullHashNumber(ctx.entryIndex) == INVALID_FULL_HASH) {
            // entry is already deleted
            // value reference is marked deleted and full hash is invalid, the last stages are done
            ctx.entryState = EntryState.DELETED;
            return false;
        }

        assert ctx.entryState == EntryState.DELETED_NOT_FINALIZED;

        // marking the delete bit in the key's off-heap header (only one true setter gets result TRUE)
        // The marking happens only when no lock is taken, otherwise busy waits
        // if the version is already different result is RETRY, if already deleted - FALSE
        // we continue anyway, therefore disregard the logicalDelete() output
        ctx.key.s.logicalDelete();

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
            ctx.value.getSlice().release();
            ctx.value.invalidate();
        }

        // mark key reference as deleted
        long expectedKeyReference = ctx.key.getSlice().getReference();
        long newKeyReference = keysMemoryManager.alterReferenceForDelete(expectedReference);
        if (casEntryFieldLong(ctx.entryIndex, KEY_REF_OFFSET, expectedKeyReference, newKeyReference)) {
            assert keysMemoryManager.isReferenceConsistent(getKeyReference(ctx.entryIndex));
            ctx.key.getSlice().release();
            ctx.key.invalidate();
        }

        if (casFullHashEntryIndex(ctx.entryIndex, ctx.fullHash, INVALID_FULL_HASH)) {
            numOfEntries.getAndDecrement();
            ctx.entryState = EntryState.DELETED;
        }

        return true;

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
