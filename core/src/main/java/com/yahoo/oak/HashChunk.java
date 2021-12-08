/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicInteger;

/* HashChunk keeps a set of entries placed according to the key hash.
 * Entry is reference to key and value, both located off-heap.
 * It provides access, updates and manipulation on each entry, provided its entry index.
 *
 * Entry is a set of 3 fields (consecutive longs), part of "entries" long array.
 * The definition of each long is explained below. Also bits of each long (references) are represented
 * in a very special way (also explained below). The bits manipulations are ReferenceCodec responsibility.
 * Please update with care.
 *
 * Entries Array:
 * --------------------------------------------------------------------------------------
 * 0 | Key Reference          | Reference encoding all info needed to           |
 *   |                        | access the key off-heap                         | entry with
 * -----------------------------------------------------------------------------| entry index
 * 1 | Value Reference        | Reference encoding all info needed to           | 0
 *   |                        | access the value off-heap. (The encoding        |
 *   |                        | algorithm of key and value can be different)    |
 * -----------------------------------------------------------------------------|
 * 2 | Hash  - key hash, which might be different (bigger) than                 |
 *   |         entry index + update counter (increased every update)            |
 * ---------------------------------------------------------------------------------------
 * 3 | Key Reference          | Reference encoding all access info.             |
 *   |                        | Deleted reference (still) encoding all info     | entry with
 *   |                        | needed to access the key off-heap               | entry index
 * -----------------------------------------------------------------------------| 1
 * 4 | Value Reference        | Reference encoding all access info              |
 *   |                        |                                                 | entry can be
 * -----------------------------------------------------------------------------| deleted
 * 5 | Hash  - key hash, which might be different (bigger) than                 |
 *   |         entry index + update counter (increased every update)            |
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
class HashChunk<K, V> extends Chunk<K, V> {
    // defaults
    public static final int HASH_CHUNK_MAX_ITEMS_DEFAULT = 2048; //2^11

    // HashChunk takes a number of least significant bits from the full key hash
    // to provide as an index in the EntryHashSet
    private final UnionCodec hashIndexCodec; // to be given

    /**
     * This constructor is only used when creating the first ever chunk/chunks (without a creator).
     * The caller might set the creator before returning the HashChunk to the user.
     *
     * @param entriesCapacity  is the size of the entries array (not all the entries are going to be in use)
     *                         IMPORTANT!: it is better to be a power of two,
     *                         if not the rest of the entries are going to be waisted
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
    HashChunk(OakSharedConfig<K, V> config, int entriesCapacity, UnionCodec hashIndexCodec) {
        // must be called after setSecondLevelBitsThreshold
        super(config, ADDITIONAL_FIELDS, entriesCapacity);
        assert Math.pow( 2, hashIndexCodec.getFirstBitSize() ) <= entriesCapacity ;
        this.hashIndexCodec = hashIndexCodec;
    }

    /**
     * Create a child HashChunk where this HashChunk object as its creator.
     */
    HashChunk<K, V> createChild(UnionCodec hashIndexCodec) {
        HashChunk<K, V> child = new HashChunk<>(config, array.entryCount(), hashIndexCodec);
        updateBasicChild(child);
        return child;
    }

    private int calculateKeyHash(ThreadContext ctx, K key) {
        if (ctx.operationKeyHash == HashChunk.INVALID_KEY_HASH) {
            // should happen only during component unit test!
            ctx.operationKeyHash = Math.abs(key.hashCode());
        }
        // hash was already calculated by hash array when looking for the chunk and kept in thread context
        return ctx.operationKeyHash;
    }


    /********************************************************************************************/
    /*-----------------------------  Wrappers for EntryOrderedSet methods -----------------------------*/

    /**
     * Writes the key off-heap and allocates an entry with the reference pointing to the given key
     * See {@code EntryHashSet.allocateEntryAndWriteKey(ThreadContext)} for more information
     * Returns false if rebalance is required and true otherwise
     */
    @Override
    boolean allocateEntryAndWriteKey(ThreadContext ctx, K key) {
        int keyHash = calculateKeyHash(ctx, key);
        int keyIndex = calculateEntryIdx(keyHash);
        return allocateEntryAndWriteKey( ctx, key, keyIndex, keyHash);
    }

    /********************************************************************************************/
    /*-----------------------  Methods for looking up item in this chunk -----------------------*/

    private int calculateEntryIdx(int keyHash) {
        // hashIndexCodec in chunk (in different chunks) and in FirstLevelHashArray may differ
        // first and not second, because these are actually the least significant bits
        return hashIndexCodec.getFirst(keyHash);
    }

    /**
     * Look up a key in this chunk.
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
        int keyHash = calculateKeyHash(ctx, key);
        int idx = calculateEntryIdx(keyHash);
        lookUp(ctx, key, idx, keyHash);
    }

    /**
     * Look up a key in this chunk. Look only whether the key exists on the moment of checking.
     * The ctx.key and ctx.value are going to be updated in case key is found.
     * But state of the entry is not going to be precise only either VALID or UNKNOWN.
     * @param ctx The context that follows the operation following this key look up.
     * @param key the key to look up
     */
    boolean lookUpForGetOnly(ThreadContext ctx, K key) {
        int keyHash = calculateKeyHash(ctx, key);
        int idx = calculateEntryIdx(keyHash);
        return lookUpForGetOnly(ctx, key, idx, keyHash);
    }

    /********************************************************************************************/
    /*---------- Methods for managing the put/remove path of the keys and values  --------------*/

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
        if (writeValueCommit(ctx) == ValueUtils.ValueResult.FALSE) {
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


    /** {@inheritDoc} **/
    @Override
    boolean shouldRebalance() {
        //TODO: no rebalance for now, add later
        return false;
    }

    void printSummaryDebug() {
        System.out.print(" Entries: " + statistics.getTotalCount() + ", capacity: "
            + array.entryCount() + ", collisions: "
            + getCollisionChainLength() + ", average accesses: ");
    }

    /********************************************************************************************/
    /*--------------------------------- Iterators Constructors ---------------------------------*/
    // TODO: update hash iterator later


    /* ####################################################
       # EntryHashSet
       #################################################### */

    /*-------------- Constants --------------*/

    static final int DEFAULT_COLLISION_CHAIN_LENGTH = 4;
    // HASH - the key hash of this entry (long includes the update counter)
    private static final int HASH_FIELD_OFFSET = 2;
    private static final int KEY_HASH_BITS = 32; // Hash needs to be an integer
    private static final int UPDATE_COUNTER_BITS = 31;

    // key hash may have any integer value including zero. However, initially
    // all array's memory is zeroed, including key hashes and their update counters.
    // This is invalid field for the entire keyHash field including update counter.
    // Used for ThreadContext initialization only
    static final long INVALID_KEY_HASH_AND_UPD_CNT = 0L;
    static final int INVALID_KEY_HASH = -1; // EntryHashSet require positive hashes

    // Additional field to keep key hash + its update counter (additional to the key and value reference fields)
    // # of additional primitive fields in each item of entries array
    private static final int ADDITIONAL_FIELDS = 1;

    // number of entries candidates to try in case of collision
    private final AtomicInteger collisionChainLength = new AtomicInteger(DEFAULT_COLLISION_CHAIN_LENGTH);


    // use union codec to encode key hash integer (first) with its update counter (second)
    private static final UnionCodec HASH_CODEC = new UnionCodec(
            KEY_HASH_BITS, // bits# to represent key hash as any integer
            UPDATE_COUNTER_BITS, // bits# to represent update counter in less than an integer
            UnionCodec.AUTO_CALCULATE_BIT_SIZE, // valid bit
            Long.SIZE);


    /*---------- Private methods for managing key hash entry field -------------*/
    /**
     * getKeyHash returns the key hash of the entry given by entry index "ei"
     * (without update counter!).
     */
    private int getKeyHash(int ei) {
        assert isIndexInBound(ei);
        // extract the hash number (first) from the updates counter (second)
        return HASH_CODEC.getFirst(getKeyHashAndUpdateCounter(ei));
    }

    /**
     * getKeyHashAndUpdateCounter returns the key hash (the entire long including the update counter!)
     * of the entry given by entry index "ei".
     */
    private long getKeyHashAndUpdateCounter(int ei) {
        assert isIndexInBound(ei);
        return array.getEntryFieldLong(ei, HASH_FIELD_OFFSET);
    }

    /**
     * isKeyHashValid checks the key hash valid bit, disregarding the update counter, when it is above zero.
     *
     * key hash may have any integer value including zero. However, initially
     * all array's memory is zeroed, including key hashes and their update counters.
     */
    private boolean isKeyHashValid(int ei) {
        long keyHashField = getKeyHashAndUpdateCounter(ei);
        return (keyHashField != 0) && (HASH_CODEC.getThird(keyHashField) == 1);
    }

    /**
     * casKeyHashAndUpdateCounter CAS the key hash including the update counter
     * (of the entry given by entry index "ei") to be
     * the `newKeyHash` only if it was `oldKeyHash`, the update counter incorporated inside
     * `oldKeyHash` must match. Also update counter is increased as part of the action.
     */
    private boolean casKeyHashAndUpdateCounter(int ei, long oldKeyHash, int newKeyHash) {
        // extract the updates counter from the old hash and increase it and to add to the new hash
        int updCnt = HASH_CODEC.getSecond(oldKeyHash);
        long newFullHashField = // last one, means setting the valid bit
                HASH_CODEC.encode(newKeyHash, updCnt + 1, 1);
        return array.casEntryFieldLong(ei, HASH_FIELD_OFFSET, oldKeyHash, newFullHashField);
    }

    /**
     * invalidateKeyHashAndUpdateCounter turns on the invalid bit (of the entry given by entry
     * index "ei") using CAS.
     * The key hash remains the same, the update counter is increased.
     * The 'oldKeyHash' must match the field value.
     */
    private boolean invalidateKeyHashAndUpdateCounter(int ei, long oldKeyHash) {
        // extract the updates counter from the old hash and increase it and to add to the new hash
        int updCnt = HASH_CODEC.getSecond(oldKeyHash);
        int keyHash = HASH_CODEC.getFirst(oldKeyHash);
        long newKeyHash = // last zero, means re-setting the valid bit
                HASH_CODEC.encode(keyHash, updCnt + 1, 0);
        return array.casEntryFieldLong(ei, HASH_FIELD_OFFSET, oldKeyHash, newKeyHash);
    }

    /*----- Private Helpers -------*/
    /**
     * Compare an object key with a serialized key that is pointed by a specific entry index,
     * and say whether they are equal. The comparison starts with hash of two keys compare,
     * only if serialized key's hash is valid.
     *
     * IMPORTANT:
     *  1. Assuming the entry's key is already read into the tempKeyBuff
     *
     * @param tempKeyBuff a reusable buffer object for internal temporary usage
     *                    As a side effect, this buffer will contain the compared
     *                    serialized key.
     * @param key         the key to compare
     * @param idx         the entry index to compare with
     * @param keyHash     the hash of the object key without update counter
     * @return true - the keys are equal, false - otherwise
     */
    private boolean isKeyAndEntryKeyEqual(KeyBuffer tempKeyBuff, K key, int idx, int keyHash) {

        // check the key's hash comparison first
        if (isKeyHashValid(idx)) {
            int entryKeyHash = getKeyHash(idx);
            if (entryKeyHash != keyHash) {
                return false;
            }
        }

        assert tempKeyBuff.isAssociated();
        return (0 == comparator.compareKeyAndSerializedKey(key, tempKeyBuff));
    }

    /* Check the state of the entry in `idx`
     ** Assume upon invocation that ctx.value, ctx.key, and ctx.keyHash are invalidated
     ** At the end:
     ** If output is  EntryState.UNKNOWN --> ctx.key, ctx.value remain untouched
     ** If output is  EntryState.DELETED_NOT_FINALIZED/DELETED/INSERT_NOT_FINALIZED/VALID
     ** --> ctx.key, ctx.value, ctx.keyHash keep the data of the entry's key, value, and key hash
     */
    private EntryState getEntryState(ThreadContext ctx, int idx, K key, int keyHash) {

        ctx.keyHashAndUpdateCnt = getKeyHashAndUpdateCounter(idx);
        if (getKeyReference(idx) == keysMemoryManager.getInvalidReference()) {
            return EntryState.UNKNOWN;
        }
        // entry was used already, is value valid or deleted?
        if (getValueReference(idx) != valuesMemoryManager.getInvalidReference()) {
            // the linearization point of deletion is marking the value off-heap
            // isValueDeleted checks the reference first (for being deleted) than the off-heap header
            // isValueDeleted returns true also for invalid reference therefore the check above
            if (isValueDeleted(ctx.value, idx)) { // value is read to the ctx.value as a side effect
                // for later progressing with deleted entry read current key slice
                // (value is read during deleted key check, unless deleted)
                if (readKey(ctx.key, idx)) {
                    // key is not deleted: either this deletion is not yet finished,
                    // or this is a new assignment on top of deleted entry
                    // Check the key hash:
                    // if invalid --> this is INSERT_NOT_FINALIZED if valid --> DELETED_NOT_FINALIZED
                    if (isKeyHashValid(idx)) {
                        return EntryState.DELETED_NOT_FINALIZED;
                    }
                    // INSERT_NOT_FINALIZED will be returned later if key matches
                } else {
                    // key is deleted, check that key hash is invalidated,
                    // because it is the last stage of deletion
                    return isKeyHashValid(idx) ? EntryState.DELETED_NOT_FINALIZED : EntryState.DELETED;
                }
                // not finalized insert is progressing out of this if
            }
        }

        // read current key slice (value is read during delete check)
        if (!readKey(ctx.key, ctx.entryIndex)) {
            // key is deleted (was already checked for being valid, cannot turn to be invalid again)
            // check that key hash is invalidated, because it is the last stage of deletion
            return isKeyHashValid(idx) ? EntryState.DELETED_NOT_FINALIZED : EntryState.DELETED;
        }

        // value is either invalid or valid (but not deleted), can be in progress of being inserted
        // if value reference is invalid or deleted, readValue returns false
        if (readValue(ctx.value, idx)) {
            return EntryState.VALID;
        }

        // value is invalid
        if (isKeyAndEntryKeyEqual(ctx.key, key, idx, keyHash)) {
            return EntryState.INSERT_NOT_FINALIZED;
        }
        return EntryState.VALID;
    }

    int getCollisionChainLength() {
        return collisionChainLength.get();
    }

    /**
     * lookUp checks whether key exists in the given idx or after.
     * Given initial index for the key, it checks entries[idx] first and continues
     * to the next entries up to 'collisionChainLength', if key wasn't previously found.
     * If true is returned, ctx.entryIndex keeps the index of the found entry,
     * ctx.entryState keeps the state, and key and value are read into the ctx.
     *
     * @param ctx the context that will follow the operation following this key allocation
     * @param key the key to write
     * @param idx idx=keyHash||bit_size_mask
     * @param keyHash keyHash=hashFunction(key)
     * @return true only if the key is found (ctx.entryState keeps more details).
     *         Otherwise (false), key wasn't found, ctx with it's buffers is invalidated
     */
    boolean lookUp(ThreadContext ctx, K key, int idx, int keyHash) {
        // start from given hash index
        // and check the next `collisionChainLength` indexes if previous index is occupied
        int collisionChainLengthLocal = collisionChainLength.get();

        // as far as we didn't check more than `collisionChainLength` indexes
        for (int i = 0; i < collisionChainLengthLocal; i++) {
            ctx.invalidate(); // before checking new entry forget what was known about other entry
            ctx.entryIndex = (idx + i) % array.entryCount(); // check the entry candidate, cyclic increase
            // entry's key is read into ctx.tempKey as a side effect
            ctx.entryState = getEntryState(ctx, ctx.entryIndex, key, keyHash);

            if (ctx.entryState == EntryState.UNKNOWN) {
                ctx.invalidate();
                return false; // there is no such a key and there is no need to look forward
            }

            // value and key slices are read during getEntryState() unless the entry is
            // deleted (EntryState.DELETED/EntryState.DELETED_NOT_FINALIZED)
            // in this case we cannot compare the key (!)
            // also deletion linearization point is checked during getEntryState()
            if (ctx.entryState != EntryState.DELETED &&
                    ctx.entryState != EntryState.DELETED_NOT_FINALIZED &&
                    isKeyAndEntryKeyEqual(ctx.key, key, ctx.entryIndex, keyHash)) {
                // EntryState.VALID --> the key is found
                // DELETED_NOT_FINALIZED --> key doesn't exists
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
     * lookUpForGetOnly checks whether key exists in the given idx or after.
     * Given initial index for the key, it checks entries[idx] first and continues
     * to the next entries up to 'collisionChainLength', if key wasn't previously found.
     * If true is returned, ctx.entryIndex keeps the index of the found entry,
     * ctx.entryState keeps the state, and key and value are read into the ctx.
     *
     * @param ctx the context that will follow the operation following this key allocation
     * @param key the key to write
     * @param idx idx=keyHash||bit_size_mask
     * @param keyHash keyHash=hashFunction(key)
     * @return true only if the key is found (ctx.entryState keeps more details).
     *         Otherwise (false), key wasn't found, ctx with it's buffers is invalidated
     */
    boolean lookUpForGetOnly(ThreadContext ctx, K key, int idx, int keyHash) {
        // start from given hash index
        // and check the next `collisionChainLength` indexes if previous index is occupied
        int collisionChainLengthLocal = collisionChainLength.get();

        // as far as we didn't check more than `collisionChainLength` indexes
        for (int i = 0; i < collisionChainLengthLocal; i++) {
            // thread context comes invalidated
            ctx.entryIndex = (idx + i) % array.entryCount(); // check the entry candidate, cyclic increase

            long keyReference = getKeyReference(ctx.entryIndex);
            if (keyReference == keysMemoryManager.getInvalidReference()) {
                return false; // there is no such a key and there is no need to look forward
            }

            // Need to read the key here, not calling readKey() in order to save extra key reference
            // re-reading. If key reference is marked as deleted, reference decoding will fail
            // (return false), if so, entry is not in the set anyway, either the deletion is finalized
            // or not, continue to next one
            if (!ctx.key.getSlice().decodeReference(keyReference)) {
                continue;
            }

            // key reference is valid and not deleted, key is read into ctx.key. We will proceed to
            // read the value only if this is key we are looking for
            if (isKeyAndEntryKeyEqual(ctx.key, key, ctx.entryIndex, keyHash)) {
                // now it is worth to read the value, which can still be deleted
                if (!readValue(ctx.value, ctx.entryIndex)) {
                    // value reference is either deleted or invalid
                    // there can be multiple possibilities what can be the true state of the entry,
                    // either INSERT_NOT_FINALIZED or DELETED_NOT_FINALIZED or DELETED
                    // However from lookUp point of view the value (and the entry) is invalid in each of
                    // the cases
                    return false;
                }
                ctx.entryState = EntryState.VALID;
                return true;
            }

            // not in this entry, move to next
            ctx.invalidate(); // before checking new entry forget what was known about other entry
        }
        ctx.invalidate();
        return false;
    }

    /**
     * findSuitableEntryForInsert finds the entry where given hey is going to be inserted.
     * Given initial index for the key, it checks entries[idx] first and continues
     * to the next entries up to 'collisionChainLength', if the previous entries are all occupied.
     * If true is returned, ctx.entryIndex keeps the index of the chosen entry
     * and ctx.entryState keeps the state.
     *
     * Invocation needs to happen within publish/unpublish scope!
     *
     * @param ctx the context that will follow the operation following this key allocation
     * @param key the key to write
     * @param idx idx=keyHash||bit_size_mask
     * @param keyHash keyHash=hashFunction(key)
     * @return true only if the index is found (ctx.entryState keeps more details).
     *         Otherwise (false), re-balance is required
     *         ctx.entryIndex keeps the index of the chosen entry & ctx.entryState keeps the state
     */
    private boolean findSuitableEntryForInsert(ThreadContext ctx, K key, int idx, int keyHash) {
        // start from given hash index
        // and check the next `collisionChainLength` indexes if previous index is occupied
        int collisionChainLengthLocal = collisionChainLength.get();
        boolean entryFound = false;

        // as far as we didn't check more than `collisionChainLength` indexes
        for (int i = 0; !entryFound && (i < collisionChainLengthLocal); i++) {
            ctx.invalidate();
            ctx.entryIndex = (idx + i) % array.entryCount(); // check the entry candidate, cyclic increase
            // entry's key is read into ctx.tempKey as a side effect
            ctx.entryState = getEntryState(ctx, ctx.entryIndex, key, keyHash);

            boolean redoSwitch;

            do {
                redoSwitch = false;
                // EntryState.VALID --> entry is occupied, continue to next possible location
                // EntryState.DELETED_NOT_FINALIZED --> finish the deletion, then try to insert here
                // EntryState.UNKNOWN, EntryState.DELETED --> entry is vacant, try to insert the key here
                // EntryState.INSERT_NOT_FINALIZED --> you can compete to associate value with the same key
                switch (ctx.entryState) {
                    case VALID:
                        if (isKeyAndEntryKeyEqual(ctx.key, key, idx, keyHash)) {
                            // found valid entry has our key, the inserted key must be unique
                            // the entry state will indicate that insert didn't happen
                            return true;
                        }
                        break;
                    case DELETED_NOT_FINALIZED:
                        deleteValueFinish(ctx); //invocation must be within chunk publishing scope!
                        // deleteValueFinish() also changes the entry state to DELETED
                        // get the entry state again to get the deleted key & value references into context
                        ctx.entryState = getEntryState(ctx, ctx.entryIndex, key, keyHash);
                        // need to redo the switch again
                        redoSwitch = true;
                        break;
                    case DELETED: // deliberate break through
                    case UNKNOWN: // deliberate break through
                    case INSERT_NOT_FINALIZED: // continue allocating value without allocating a key
                        entryFound = true;
                        break;
                    default:
                        // For debugging the case new state is added and this code is not updated
                        assert false;
                }
                // if none of the above, it is while loop restart due to deletion finish,
                // or entry being occupied
            } while (redoSwitch); // end of the while loop
        }

        if (!entryFound) {
            // TODO: When rebalance will take place, here should come the code to decide which
            // TODO: kind of rebalance is requested: either split chunk into two, or resize to a
            // TODO: twice bigger chunk
            if (collisionChainLengthLocal == (DEFAULT_COLLISION_CHAIN_LENGTH * 10)) {
                System.out.println("WARNING!: Many collisions for the hash function");
            }
            if (collisionChainLengthLocal == (DEFAULT_COLLISION_CHAIN_LENGTH * 20)) {
                System.out.println(
                        "WARNING!: Too much collisions for the hash function may cause performance degradation");
            }
            if (collisionChainLengthLocal > array.entryCount()) {
                System.out.println(
                        "FAILURE!: Too much collisions (" + collisionChainLength.get() +
                                ") to be kept in one chunk. Make sure to enlarge chunk!");
                throw new RuntimeException();
            }
            collisionChainLength.incrementAndGet();
            // restart recursively (hopefully won't happen too much)
            return findSuitableEntryForInsert(ctx, key, idx, keyHash);
        }

        // entry state can only be DELETED or UNKNOWN or INSERT_NOT_FINALIZED
        assert (ctx.entryState == EntryState.DELETED || ctx.entryState == EntryState.UNKNOWN ||
                ctx.entryState == EntryState.INSERT_NOT_FINALIZED);
        return true;
    }

    /********************************************************************************************/
    /*------ Methods for managing the write/remove path of the hashed keys and values  ---------*/

    /**
     * Creates/allocates an entry for the key, given keyHash=hashFunction(key)
     * and idx=keyHash||bit_size_mask. An entry is always associated with a key,
     * therefore the key is written to off-heap and associated with the entry simultaneously.
     * The value of the new entry is set to NULL (INVALID_VALUE_REFERENCE)
     * or can be some deleted value
     *
     * Upon successful finishing of allocateEntryAndWriteKey():
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
     * @param idx idx=keyHash||bit_size_mask
     * @param keyHash keyHash=hashFunction(key)
     * @return true only if the allocation was successful.
     *         Otherwise (false), rebalance is required
     **/
    boolean allocateEntryAndWriteKey(ThreadContext ctx, K key, int idx, int keyHash) {
        ctx.invalidate();
        if (!isIndexInBound(idx)) {
            // cannot return "false" on illegal arguments,
            // it would just indicate unnecessary rebalnce requirement
            throw new IllegalArgumentException("Hash index out of bounds");
        }

        if (!findSuitableEntryForInsert(ctx, key, idx, keyHash)) {
            // rebalance is required
            return false;
        }

        if (ctx.entryState == EntryState.VALID || ctx.entryState == EntryState.INSERT_NOT_FINALIZED) {
            // our key exists, either in valid entry, or the key is already set in the relevant entry,
            // but value is not yet set. According to the entry
            // state either fail insertion or continue and compete on assigning the value
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

            // key reference CASed (only one should succeed) write the entry's key hash,
            // because it is used for keys comparison (invalid key hash is not used for comparison)
            if ( casKeyHashAndUpdateCounter(ctx.entryIndex, ctx.keyHashAndUpdateCnt, keyHash) ) {
                numOfEntries.getAndIncrement();
                return true;
            } else {
                // someone else proceeded with the same key if key hash is deleted we are totally late
                // check everything again
                return allocateEntryAndWriteKey(ctx, key, idx, keyHash);
            }
        }
        // CAS failed, does it failed because the same key as our was assigned?
        // Read the new key from entry again, read may fail (return false) if the new key was both
        // inserted and deleted, in the meanwhile
        if (readKey(ctx.key, ctx.entryIndex)) {
            if (isKeyAndEntryKeyEqual(ctx.key, key, idx, keyHash)) {
                return true; // continue to compete on assigning the value
            }
        }
        // CAS failed as other key was assigned (or current new key is deleted)
        // restart and look for the entry again
        return allocateEntryAndWriteKey(ctx, key, idx, keyHash);

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
     * (5) invalidate the key hash as well
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
    @Override
    boolean deleteValueFinish(ThreadContext ctx) {

        long expectedValueReference = ctx.value.getSlice().getReference();
        long expectedKeyReference = ctx.key.getSlice().getReference();
        // is it the same hash number we saw but already invalid?
        if (HASH_CODEC.getFirst(ctx.keyHashAndUpdateCnt) == getKeyHash(ctx.entryIndex)
                && !isKeyHashValid(ctx.entryIndex)) {
            // entry is already deleted, key hash is invalid, the last stages are done
            ctx.entryState = EntryState.DELETED;
            return false;
        }

        // marking the delete bit in the key's off-heap header (only one true setter gets result TRUE)
        // (Marking happens prior to reference being marked is deleted,
        // thus if reference is deleted the off-heap is already marked.)
        // The marking the delete bit happens only when no lock is taken, otherwise busy waits
        // if the version is already different result is RETRY, if already deleted - FALSE
        // we continue anyway, therefore disregard the logicalDelete() output
        boolean isKeyReferenceDeleted = keysMemoryManager.isReferenceDeleted(expectedKeyReference);
        if (!isKeyReferenceDeleted) {
            ctx.key.s.logicalDelete();
        }

        // Value's reference codec prepares the reference to be used after value is deleted
        long newValueReference = valuesMemoryManager.alterReferenceForDelete(expectedValueReference);
        // Scenario:
        // 1. The value's slice is marked as deleted off-heap and the thread that started
        //    deleteValueFinish falls asleep.
        // 2. This value's slice space is allocated once again and assigned into the same entry.
        // 3. A valid value reference is CASed to invalid.
        //
        // This is ABA problem and resolved via always changing deleted variation of the reference
        // Also value's off-heap slice is released to memory manager only after deleteValueFinish
        // is done.
        if (!valuesMemoryManager.isReferenceDeleted(expectedValueReference)) {
            if (array.casEntryFieldLong(ctx.entryIndex, VALUE_REF_OFFSET, expectedValueReference,
                    newValueReference)) {
                // the deletion of the value and its release should be successful only once and for one
                // thread, therefore the reference and slice should be still valid here
                assert valuesMemoryManager.isReferenceConsistent(getValueReference(ctx.entryIndex));
                assert ctx.value.isAssociated();
                ctx.value.getSlice().release();
                ctx.value.invalidate();
            }
        }
        // mark key reference as deleted, if needed
        if (!isKeyReferenceDeleted) {
            long newKeyReference = keysMemoryManager.alterReferenceForDelete(expectedKeyReference);
            if (array.casEntryFieldLong(ctx.entryIndex, KEY_REF_OFFSET, expectedKeyReference,
                    newKeyReference)) {
                assert keysMemoryManager.isReferenceConsistent(getKeyReference(ctx.entryIndex));
                ctx.key.getSlice().release();
                ctx.key.invalidate();
            }
        }

        if (invalidateKeyHashAndUpdateCounter(ctx.entryIndex, ctx.keyHashAndUpdateCnt)) {
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
}
