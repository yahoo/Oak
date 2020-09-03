/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

/* EntrySet keeps a set of entries. Entry is reference to key and value, both located off-heap.
 * EntrySet provides access, updates and manipulation on each entry, provided its index.
 *
 * IMPORTANT: Due to limitation in amount of bits we use to represent Block ID the amount of memory
 * supported by ALL OAKs in one system is 128GB only!
 * TODO: this limitation will be removed once void memory manager fully is presented for keys
 *
 * Entry is a set of 6 (FIELDS) consecutive integers, part of "entries" int array. The definition
 * of each integer is explained below. Also bits of each integer are represented in a very special
 * way (also explained below). The bits manipulations are ReferenceCodec responsibility.
 * Please update with care.
 *
 * Entries Array:
 * --------------------------------------------------------------------------------------
 * 0 | headNextIndex keeps entry index of the first ordered entry
 * --------------------------------------------------------------------------------------
 * 1 | NEXT  - entry index of the entry following the entry with entry index 1  |
 * -----------------------------------------------------------------------------|
 * 2 | KEY_REFERENCE          | these 2 integers together, represented as long  | entry with
 * --|                        | provide KEY_REFERENCE. Pay attention that       | entry index
 * 3 |                        | KEY_REFERENCE is different than VALUE_REFERENCE | 1
 * -----------------------------------------------------------------------------|
 * 4 | VALUE_REFERENCE        | these 2 integers together, represented as long  | entry that
 * --|                        | provide VALUE_REFERENCE. Pay attention that     | was allocated
 * 5 |                        | VALUE_REFERENCE is different than KEY_REFERENCE | first
 * -----------------------------------------------------------------------------|
 * 6 | NOT IN USE:             saved for alignment and future usage             |
 * --------------------------------------------------------------------------------------
 * 7 | NEXT  - entry index of the entry following the entry with entry index 2  |
 * -----------------------------------------------------------------------------|
 * 8 | KEY_REFERENCE          | these 2 integers together, represented as long  | entry with
 * --|                        | provide KEY_REFERENCE. Pay attention that       | entry index
 * 9 |                        | KEY_REFERENCE is different than VALUE_REFERENCE | 2
 * -----------------------------------------------------------------------------|
 * ...
 *
 *
 * Internal class, package visibility
 */
class EntrySet<K, V> {

    /*-------------- Constants --------------*/

    /***
     * This enum is used to access the different fields in each entry.
     * The value associated with each entry signifies the offset of the field relative to the entry's beginning.
     */
    private enum OFFSET {
        /***
         * NEXT - the next index of this entry (one integer). Must be with offset 0, otherwise, copying an entire
         * entry should be fixed (In function {@code copyPartNoKeys}, search for "LABEL").
         *
         * KEY_REFERENCE - the blockID, length and offset of the value pointed from this entry (size of two
         * integers, one long).
         *
         * VALUE_REFERENCE - the blockID, offset and version of the value pointed from this entry (size of two
         * integers, one long). Equals to INVALID_REFERENCE if no value is point.
         */
        NEXT(0), KEY_REFERENCE(1), VALUE_REFERENCE(3);

        final int value;

        OFFSET(int value) {
            this.value = value;
        }
    }

    static final int INVALID_ENTRY_INDEX = 0;

    // location of the first (head) node - just a next pointer (always same value 0)
    private final int headNextIndex = 0;

    // index of first item in array, after head (not necessarily first in list!)
    private static final int HEAD_NEXT_INDEX_SIZE = 1;

    private static final int FIELDS = 6;  // # of fields in each item of entries array

    private static final Unsafe UNSAFE = UnsafeUtils.unsafe;
    final MemoryManager valuesMemoryManager;
    final MemoryManager keysMemoryManager;
    private final int[] entries;    // array is initialized to 0 - this is important!
    private final int entriesCapacity; // number of entries (not ints) to be maximally held

    private final AtomicInteger nextFreeIndex;    // points to next free index of entry array
    // counts number of entries inserted & not deleted, pay attention that not all entries counted
    // in number of entries are finally linked into the linked list of the chunk above
    // and participating in holding the "real" KV-mappings, the "real" are counted in Chunk
    private final AtomicInteger numOfEntries;
    // for writing the keys into the off-heap bytebuffers (Blocks)
    final OakSerializer<K> keySerializer;
    final OakSerializer<V> valueSerializer;

    final ValueUtils valOffHeapOperator; // is used for any value off-heap metadata access

    /*----------------- Constructor -------------------*/

    /**
     * Create a new EntrySet
     *  @param vMM   for values off-heap allocations and releases
     * @param kMM off-heap allocations and releases for keys
     * @param entriesCapacity how many entries should this EntrySet keep at maximum
     * @param keySerializer   used to serialize the key when written to off-heap
     */
    EntrySet(MemoryManager vMM, MemoryManager kMM, int entriesCapacity, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer, ValueUtils valOffHeapOperator) {
        this.valuesMemoryManager = vMM;
        this.keysMemoryManager = kMM;
        this.entries = new int[entriesCapacity * FIELDS + HEAD_NEXT_INDEX_SIZE];
        this.nextFreeIndex = new AtomicInteger(HEAD_NEXT_INDEX_SIZE);
        this.numOfEntries = new AtomicInteger(0);
        this.entriesCapacity = entriesCapacity;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.valOffHeapOperator = valOffHeapOperator;
    }

    enum ValueState {
        /*
         * The state of the value is yet to be checked.
         */
        UNKNOWN,

        /*
         * There is an entry with the given key and its value is deleted.
         */
        DELETED,

        /*
         * When off-heap value is marked deleted, but not the value reference in the entry.
         * Deletion consists of 2 steps: (1) mark off-heap deleted (LP),
         * (2) CAS value reference to deleted
         * If not all two steps are done entry can not be reused for new insertion.
         */
        DELETED_NOT_FINALIZED,

        /*
         * There is any entry with the given key and its value is valid.
         * valueSlice is pointing to the location that is referenced by valueReference.
         */
        VALID;

        /**
         * We consider a value to be valid if it was inserted (or in the process of being inserted).
         *
         * @return is the value valid
         */
        boolean isValid() {
            return this.ordinal() >= ValueState.VALID.ordinal();
        }
    }

    /**
     * getNumOfEntries returns the number of entries allocated and not deleted for this EntrySet.
     * Although, in case EntrySet is used as an array, nextFreeIndex is can be used to calculate
     * number of entries, additional variable is used to support OakHash
     */
    int getNumOfEntries() {
        return numOfEntries.get();
    }

    int getLastEntryIndex() {
        return nextFreeIndex.get();
    }


    /********************************************************************************************/
    /*---------------- Methods for setting and getting specific entry's field ------------------*/

    /**
     * Converts external entry-index to internal array index.
     * <p>
     * We use the following terminology:
     *  - intIdx for the integer's index inside the entries array (referred as a set of integers)
     *  - entryIdx for the index of entry array (referred as a set of entries)
     *
     * @param entryIdx external entry-index
     * @return the internal array index
     */
    private static int entryIdx2intIdx(int entryIdx) {
        if (entryIdx == 0) { // assumed it is a head pointer access
            return 0;
        }
        return ((entryIdx - 1) * FIELDS) + HEAD_NEXT_INDEX_SIZE;
    }

    /**
     * getEntryArrayFieldInt gets the integer field of entry at specified offset for given
     * start of the entry index in the entry array.
     * The field is read atomically as it is a machine word, however the concurrency of the
     * mostly updated value is not ensured as no memory fence is issued.
     */
    private int getEntryArrayFieldInt(int intFieldIdx, OFFSET offset) {
        return entries[intFieldIdx + offset.value]; // used for NEXT
    }

    /**
     * getEntryArrayFieldLong atomically reads two integers field of the entries array.
     * Should be used with OFFSET.VALUE_REFERENCE and OFFSET.KEY_REFERENCE
     */
    private long getEntryArrayFieldLong(int intStartFieldIdx, OFFSET offset) {
        assert offset == OFFSET.VALUE_REFERENCE || offset == OFFSET.KEY_REFERENCE;
        long arrayOffset =
                Unsafe.ARRAY_INT_BASE_OFFSET + (intStartFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE;
        assert arrayOffset % 8 == 0;
        return UNSAFE.getLong(entries, arrayOffset);
    }

    /**
     * setEntryFieldInt sets the integer field of specified offset to 'value'
     * for given integer index in the entry array
     */
    private void setEntryFieldInt(int intFieldIdx, OFFSET offset, int value) {
        assert intFieldIdx + offset.value >= 0;
        entries[intFieldIdx + offset.value] = value; // used for NEXT
    }

    private void setEntryFieldLong(int item, OFFSET offset, long value) {
        long arrayOffset = Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE;
        assert arrayOffset % 8 == 0;
        UNSAFE.putLong(entries, arrayOffset, value);
    }

    /**
     * casEntriesArrayInt performs CAS of given integer field of the entries ints array,
     * that should be associated with some field of some entry.
     * CAS from 'expectedIntValue' to 'newIntValue' for field at specified offset
     * of given int-field in the entries array
     */
    private boolean casEntriesArrayInt(int intFieldIdx, OFFSET offset, int expectedIntValue, int newIntValue) {
        return UNSAFE.compareAndSwapInt(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (intFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expectedIntValue, newIntValue);
    }

    /**
     * casEntriesArrayLong performs CAS of given two integers field of the entries ints array,
     * that should be associated with some 2 consecutive integer fields of some entry.
     * CAS from 'expectedLongValue' to 'newLongValue' for field at specified offset
     * of given int-field in the entries array
     */
    private boolean casEntriesArrayLong(int intStartFieldIdx, OFFSET offset, long expectedLongValue,
                                        long newLongValue) {
        return UNSAFE.compareAndSwapLong(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (intStartFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expectedLongValue, newLongValue);
    }


    /********************************************************************************************/
    /*--------------- Methods for setting and getting specific key and/or value ----------------*/

    /**
     * Atomically reads the key reference from the entry (given by entry index "ei")
     */
    private long getKeyReference(int ei) {
        return getEntryArrayFieldLong(entryIdx2intIdx(ei), OFFSET.KEY_REFERENCE);
    }

    /**
     * Atomically reads the value reference from the entry (given by entry index "ei")
     */
    private long getValueReference(int ei) {
        return getEntryArrayFieldLong(entryIdx2intIdx(ei), OFFSET.VALUE_REFERENCE);
    }

    /*
     * isValueRefValidAndNotDeleted is used only to check whether the value reference, which is part of the
     * entry on entry index "ei" is valid and not deleted. No off-heap value deletion mark check.
     * Reference being marked as deleted is checked.
     *
     * Pay attention that (given entry's) value may be deleted asynchronously by other thread just
     * after this check. For the thread safety use a copy of value reference.
     * */
    boolean isValueRefValidAndNotDeleted(int ei) {
        long valRef = getValueReference(ei);
        return valuesMemoryManager.isReferenceValid(valRef)
            && !valuesMemoryManager.isReferenceDeleted(valRef);
    }

    /********************************************************************************************/
    /*------------- Methods for managing next entry indexes (package visibility) ---------------*/

    /**
     * getNextEntryIndex returns the next entry index (of the entry given by entry index "ei")
     * The method serves external EntrySet users.
     */
    int getNextEntryIndex(int ei) {
        if (ei == INVALID_ENTRY_INDEX) {
            return INVALID_ENTRY_INDEX;
        }
        return getEntryArrayFieldInt(entryIdx2intIdx(ei), OFFSET.NEXT);
    }

    /**
     * getHeadEntryIndex returns the entry index of the entry first in the array,
     * which is written in the first integer of the array
     * The method serves external EntrySet users.
     */
    int getHeadNextIndex() {
        return getEntryArrayFieldInt(headNextIndex, OFFSET.NEXT);
    }

    /**
     * setNextEntryIndex sets the next entry index (of the entry given by entry index "ei")
     * to be the "next". Input parameter "next" must be a valid entry index (not integer index!).
     * The method serves external EntrySet users.
     */
    void setNextEntryIndex(int ei, int next) {
        assert ei <= nextFreeIndex.get() && next <= nextFreeIndex.get();
        setEntryFieldInt(entryIdx2intIdx(ei), OFFSET.NEXT, next);
    }

    /**
     * casNextEntryIndex CAS the next entry index (of the entry given by entry index "ei") to be the
     * "nextNew" only if it was "nextOld". Input parameter "nextNew" must be a valid entry index.
     * The method serves external EntrySet users.
     */
    boolean casNextEntryIndex(int ei, int nextOld, int nextNew) {
        return casEntriesArrayInt(entryIdx2intIdx(ei), OFFSET.NEXT, nextOld, nextNew);
    }


    /********************************************************************************************/
    /*----- Methods for managing the read path of keys and values of a specific entry' ---------*/

    /**
     * Reads a key from entry at the given entry index (from off-heap).
     * Returns false if:
     *   (1) there is no such entry or
     *   (2) entry has no key set
     *
     * @param key the buffer that will contain the key
     * @param ei  the entry index to read
     * @return true if the entry index has a valid key allocation reference
     */
    boolean readKey(KeyBuffer key, int ei) {
        if (ei == INVALID_ENTRY_INDEX) {
            key.invalidate();
            return false;
        }

        long reference = getKeyReference(ei);
        return keysMemoryManager.decodeReference(key, reference);
    }

    /**
     * Reads a value from entry at the given entry index (from off-heap).
     * Returns false if:
     *   (1) there is no such entry or
     *   (2) entry has no value set
     *
     * @param value the buffer that will contain the value
     * @param ei    the entry index to read
     * @return  true if the entry index has a valid value reference
     *          (No check for off-heap deleted bit!)
     */
    boolean readValue(ValueBuffer value, int ei) {
        if (ei == INVALID_ENTRY_INDEX) {
            value.invalidate();
            return false;
        }
        long reference = getValueReference(ei);
        return valuesMemoryManager.decodeReference(value, reference);
    }


    /********************************************************************************************/
    /* Methods for managing the entry context of the keys and values inside ThreadContext       */

    /**
     * Updates the key portion of the entry context inside {@code ctx} that matches its entry context index.
     * Thus, {@code ctx.initEntryContext(int)} should be called prior to this method on this {@code ctx} instance.
     *
     * @param ctx the context that will be updated and follows the operation with this key
     */
    void readKey(ThreadContext ctx) {
        readKey(ctx.key, ctx.entryIndex);
    }

    /**
     * Updates the value portion of the entry context inside {@code ctx} that matches its entry context index.
     * This includes both the value itself, and the value's state.
     * Thus, {@code ctx.initEntryContext(int)} should be called prior to this method on this {@code ctx} instance.
     *
     * @param ctx the context that was initiated by {@code readKey(ctx, ei)}
     */
    void readValue(ThreadContext ctx) {
        readValue(ctx.value, ctx.entryIndex);
        ctx.valueState = getValueState(ctx.value);
        assert valuesMemoryManager.isReferenceConsistent(ctx.value.reference);
    }

    /**
     * Find the state of a the value that is pointed by {@code value}.
     * Thus, {@code readValue(value, ei)} should be called prior to this method with the same {@code value} instance.
     *
     * @param value a buffer object that contains the value buffer
     */
    private ValueState getValueState(ValueBuffer value) {
        // value can be deleted or in the middle of being deleted
        //   remove: (1)off-heap delete bit, (2)reference deleted
        //   middle state: off-heap header marked deleted, but valid reference

        if (!valuesMemoryManager.isReferenceValid(value.reference)) {
            // if there is no value associated with given key,
            // thebvalue of this entry was never yet allocated
            return ValueState.UNKNOWN;
        }

        if (valuesMemoryManager.isReferenceDeleted(value.reference)) {
            // if value is valid the reference can still be deleted
            return  ValueState.DELETED;
        }

        // value reference is valid, just need to check if off-heap is marked deleted
        ValueUtils.ValueResult result = valOffHeapOperator.isValueDeleted(value);

        // If result == TRUE, there is a deleted value associated with the given key
        // If result == RETRY, we ignore it, since it will be discovered later down the line as well
        return (result == ValueUtils.ValueResult.TRUE) ? ValueState.DELETED_NOT_FINALIZED : ValueState.VALID;
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
     *         Otherwise, it means that the EntrySet is full (may require a re-balance).
     **/
    boolean allocateEntry(ThreadContext ctx, K key) {
        ctx.invalidate();

        int ei = nextFreeIndex.getAndIncrement();
        if (ei > entriesCapacity) {
            return false;
        }
        numOfEntries.getAndIncrement();

        ctx.entryIndex = ei;
        writeKey(ctx, key);
        return true;
    }

    /**
     * Allocate and serialize a key object to off-heap KeyBuffer.
     *
     * @param key       the key to write
     * @param keyBuffer the off-heap KeyBuffer to update with the new allocation
     */
    void allocateKey(K key, KeyBuffer keyBuffer) {
        int keySize = keySerializer.calculateSize(key);
        keysMemoryManager.allocate(keyBuffer, keySize);
        ScopedWriteBuffer.serialize(keyBuffer, key, keySerializer);
    }

    /**
     * Allocate a new KeyBuffer and duplicate an existing key to the new one.
     *
     * @param src the off-heap KeyBuffer to copy from
     * @param dst the off-heap KeyBuffer to update with the new allocation
     */
    void duplicateKey(KeyBuffer src, KeyBuffer dst) {
        final int keySize = src.capacity();
        keysMemoryManager.allocate(dst, keySize);

        // We duplicate the buffer without instantiating a write buffer because the user is not involved.
        UnsafeUtils.unsafe.copyMemory(src.getAddress(), dst.getAddress(), keySize);
    }

    /**
     * Writes given key object "key" (to off-heap) as a serialized key, referenced by entry
     * that was set in this context ({@code ctx}).
     *
     * @param ctx the context that follows the operation since the key was found/created
     * @param key the key to write
     **/
    private void writeKey(ThreadContext ctx, K key) {
        allocateKey(key, ctx.key);

        /*
        The current entry key reference should be updated. The value reference should be invalid.
        In reality, the value reference is already set to zero,
        because the entries array is initialized that way (see specs).
         */
        setEntryFieldLong(entryIdx2intIdx(ctx.entryIndex),
            OFFSET.KEY_REFERENCE, keysMemoryManager.encodeReference(ctx.key));
    }

    /**
     * Writes value off-heap. Supposed to be for entry index inside {@code ctx},
     * but this entry metadata is not updated in this method. This is an intermediate step in
     * the process of inserting key-value pair, it will be finished with {@code writeValueCommit(ctx}.
     * The off-heap header is initialized in this function as well.
     *
     * @param ctx          the context that follows the operation since the key was found/created
     * @param value        the value to write off-heap
     * @param writeForMove true if the value will replace another value
     **/
    void writeValueStart(ThreadContext ctx, V value, boolean writeForMove) {
        // the length of the given value plus its header
        int valueDataSize   = valueSerializer.calculateSize(value);
        int valueLength     = valueDataSize + valOffHeapOperator.getHeaderSize();

        // The allocated slice includes all the needed information for further access
        valuesMemoryManager.allocate(ctx.newValue, valueLength);
        ctx.newValue.setReference(valuesMemoryManager.encodeReference(ctx.newValue));
        ctx.isNewValueForMove = writeForMove;

        // for value written for the first time:
        // initializing the off-heap header's lock to be free
        // for value being moved, initialize the lock to be locked
        if (writeForMove) {
            valOffHeapOperator.initLockedHeader(ctx.newValue, valueDataSize);
        } else {
            valOffHeapOperator.initHeader(ctx.newValue, valueDataSize);
        }

        ScopedWriteBuffer.serialize(ctx.newValue, value, valueSerializer);
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

        long oldValueReference = ctx.value.getReference();
        long newValueReference = ctx.newValue.getReference();
        assert valuesMemoryManager.isReferenceValid(newValueReference);

        int intIdx = entryIdx2intIdx(ctx.entryIndex);
        if (!casEntriesArrayLong(intIdx, OFFSET.VALUE_REFERENCE, oldValueReference, newValueReference)) {
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
        if (valuesMemoryManager.isReferenceDeleted(ctx.value.reference)) {
            return false; // value reference in the slice is marked deleted
        }

        assert ctx.valueState == ValueState.DELETED_NOT_FINALIZED;

        int indIdx = entryIdx2intIdx(ctx.entryIndex);

        // Value's reference codec prepares the reference to be used after value is deleted
        long expectedReference = ctx.value.getReference();
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
        if (casEntriesArrayLong(indIdx, OFFSET.VALUE_REFERENCE, expectedReference, newReference)) {
            assert valuesMemoryManager.isReferenceConsistent(getValueReference(ctx.entryIndex));
            numOfEntries.getAndDecrement();
            valuesMemoryManager.release(ctx.value);
            ctx.value.invalidate();
            ctx.valueState = ValueState.DELETED;

            return true;
        }
        // CAS wasn't successful, someone else set the value reference as deleted and
        // maybe value was allocated again. Let the context's value to be updated
        readValue(ctx);
        return false;
    }

    /**
     * Releases the key of the input context.
     * Currently in use only for unreached keys, waiting for GC to be arranged
     *
     * @param ctx the context that follows the operation since the key was found/created
     **/
    void releaseKey(ThreadContext ctx) {
        // currently using values memory manager as keys are not a subject to be released
        valuesMemoryManager.release(ctx.key);
    }

    /**
     * Releases the newly allocated value of the input context.
     * Currently the method is used only to release an
     * unreachable value reference, the one that was not yet attached to an entry!
     * The method is part of EntrySet, because it cares also
     * for writing the value before attaching it to an entry (writeValueStart/writeValueCommit)
     *
     * @param ctx the context that follows the operation since the key was found/created
     **/
    void releaseNewValue(ThreadContext ctx) {
        valuesMemoryManager.release(ctx.newValue);
    }

    /**
     * Checks if an entry is deleted (checks on-heap and off-heap).
     *
     * @param tempValue a reusable buffer object for internal temporary usage
     * @param ei        the entry index to check
     * @return true if the entry is deleted
     */
    boolean isEntryDeleted(ValueBuffer tempValue, int ei) {
        boolean isAllocatedAndNotDeleted = readValue(tempValue, ei);
        if (!isAllocatedAndNotDeleted) {
            return true;
        }
        return valOffHeapOperator.isValueDeleted(tempValue) != ValueUtils.ValueResult.FALSE;
    }


    /******************************************************************/
    /*
     * All the functionality that links entries into a linked list or updates the linked list
     * is provided by the user of the entry set. EntrySet provides the possibility to update the
     * next entry index via set or CAS, but does it only as a result of the user request.
     * */

    /**
     * copyEntry copies one entry from source EntrySet (at source entry index "srcEntryIdx") to this EntrySet.
     * The destination entry index is chosen according to this nextFreeIndex which is increased with
     * each copy. Deleted entry (marked on-heap or off-heap) is not copied (disregarded).
     * <p>
     * The next pointers of the entries are requested to be set by the user if needed.
     *
     * @param tempValue   a reusable buffer object for internal temporary usage
     * @param srcEntrySet another EntrySet to copy from
     * @param srcEntryIdx the entry index to copy from {@code srcEntrySet}
     * @return false when this EntrySet is full
     * <p>
     * Note: NOT THREAD SAFE
     */
    boolean copyEntry(ValueBuffer tempValue, EntrySet<K, V> srcEntrySet, int srcEntryIdx) {
        if (srcEntryIdx == headNextIndex) {
            return false;
        }

        // don't increase the nextFreeIndex yet, as the source entry might not be copies
        int destEntryIndex = nextFreeIndex.get();

        if (destEntryIndex > entriesCapacity) {
            return false;
        }

        if (srcEntrySet.isEntryDeleted(tempValue, srcEntryIdx)) {
            return true;
        }

        assert valuesMemoryManager.isReferenceConsistent(tempValue.reference);

        // ARRAY COPY: using next as the base of the entry
        // copy both the key and the value references and integer for future use => 5 integers via array copy
        // the first field in an entry is next, and it is not copied since it should be assigned elsewhere
        // therefore, to copy the rest of the entry we use the offset of next (which we assume is 0) and
        // add 1 to start the copying from the subsequent field of the entry.
        System.arraycopy(srcEntrySet.entries,  // source entries array
                entryIdx2intIdx(srcEntryIdx) + OFFSET.NEXT.value + 1,
                entries,                        // this entries array
                entryIdx2intIdx(destEntryIndex) + OFFSET.NEXT.value + 1, (FIELDS - 1));

        assert valuesMemoryManager.isReferenceConsistent(getValueReference(destEntryIndex));

        // now it is the time to increase nextFreeIndex
        nextFreeIndex.getAndIncrement();
        numOfEntries.getAndIncrement();
        return true;
    }

    boolean isEntrySetValidAfterRebalance() {
        int currIndex = getHeadNextIndex();
        int prevIndex = -1;
        while (currIndex > getLastEntryIndex()) {
            if (!isValueRefValidAndNotDeleted(currIndex)) {
                return false;
            }
            if (!valuesMemoryManager.isReferenceConsistent(getValueReference(currIndex))) {
                return false;
            }
            if ( prevIndex>0 && currIndex - prevIndex != FIELDS) {
                return false;
            }
            prevIndex = currIndex;
            currIndex = getNextEntryIndex(currIndex);
        }

        return true;
    }
}
