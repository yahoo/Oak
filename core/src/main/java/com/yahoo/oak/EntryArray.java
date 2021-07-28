/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

/*
* EntryArray is array of entries based on primitive array of longs.
* Entry must include at least Key Reference and Value Reference fields, but might include more.
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
 * 2 |  Optional additional entry's 0 field, set by derived class               | first
 * ---------------------------------------------------------------------------------------
 * 3 | Key Reference          | Reference encoding all info needed to           |
 *   |                        | access the key off-heap                         |
 * -----------------------------------------------------------------------------| entry with
 * 4 | Value Reference        | Reference encoding all info needed to           | entry index
 *   |                        | access the key off-heap.                        | 1
 * -----------------------------------------------------------------------------|
 * 5 | Optional additional entry's 1 field, set by derived class                |
 * ---------------------------------------------------------------------------------------
 * 6 | Key Reference          | Reference encoding all info needed to           |
 *   |                        | access the key off-heap                         |
 * -----------------------------------------------------------------------------| entry with
 * 7 | Value Reference        | Reference encoding all info needed to           | entry index
 *   |                        | access the key off-heap.                        | 2
 * -----------------------------------------------------------------------------|
 * 8 | Optional additional entry's 2 field, set by derived class                |
 * ---------------------------------------------------------------------------------------
 * ...
 *
* */
public class EntryArray<K, V> {
    /***
     * KEY_REF_OFFSET - offset in primitive fields of one entry, to a reference as coded by
     * keysMemoryManager, of the key pointed from this entry (size of long)
     *
     * VALUE_REF_OFFSET - offset in primitive fields of one entry, to a reference as coded by
     * valuesMemoryManager, of the value pointed from this entry (size of long).
     * The primitive field equals to INVALID_REFERENCE if no value is point.
     */
    protected static final int KEY_REF_OFFSET = 0;
    protected static final int VALUE_REF_OFFSET = 1;
    static final int INVALID_ENTRY_INDEX = -1;

    final MemoryManager valuesMemoryManager;
    final MemoryManager keysMemoryManager;
    private final long[] entries;    // array is initialized to 0 - this is important!
    private final int fields;  // # of primitive fields in each item of entries array

    protected final int entriesCapacity; // number of entries (not longs) to be maximally held

    // Counts number of entries inserted & not deleted. Pay attention that not all entries (counted
    // in number of entries) are finally are finally considered existing by the OrderedChunk above
    // and participating in holding the "real" KV-mappings, the "real" are counted in OrderedChunk
    protected final AtomicInteger numOfEntries;

    // for writing the keys into the off-heap
    final OakSerializer<K> keySerializer;
    final OakSerializer<V> valueSerializer;

    /**
     * Create a new instance
     * @param vMM   for values off-heap allocations and releases
     * @param kMM off-heap allocations and releases for keys
     * @param entriesCapacity how many entries should this instance keep at maximum
     * @param keySerializer   used to serialize the key when written to off-heap
     */
    EntryArray(MemoryManager vMM, MemoryManager kMM, int additionalFieldCount, int entriesCapacity,
               OakSerializer<K> keySerializer,
             OakSerializer<V> valueSerializer) {
        this.valuesMemoryManager = vMM;
        this.keysMemoryManager = kMM;
        this.fields = additionalFieldCount + 2; // +2 for key and value references that always exist
        this.entries = new long[entriesCapacity * this.fields];
        this.numOfEntries = new AtomicInteger(0);
        this.entriesCapacity = entriesCapacity;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    // EntryState is the state of the entry after inspection of the states of its key and value
    // For Example:
    // Entry in which a key is allocated, but without a valid value, is considered not existing
    // Entry marked deleted only off-heap is already considered deleted
    enum EntryState {
        /*
         * The state of the entry is yet to be checked.
         */
        UNKNOWN,

        /*
         * There is an entry with the given key and it is deleted.
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
        * For EntryArray serving for Hash, valid key reference is set, but value not yet
        * */
        INSERT_NOT_FINALIZED,

        /*
         * There is any entry with the given key and its is valid.
         * valueSlice is pointing to the location that is referenced by valueReference.
         */
        VALID;

        /**
         * We consider an entry to be valid if it was inserted and not deleted.
         *
         * @return is the value valid
         */
        boolean isValid() {
            return this.ordinal() >= EntryState.VALID.ordinal();
        }
    }


    /********************************************************************************************/
    /*---------------- Methods for setting and getting specific entry's field ------------------*/

    /**
     * Converts external entry-index to internal array index.
     * <p>
     * We use the following terminology:
     *  - longIdx for the long's index inside the entries array (referred as a set of longs)
     *  - entryIdx for the index of entry array (referred as a set of entries)
     *
     * @param entryIdx external entry-index
     * @return the internal array index
     */
    private int entryIdx2LongIdx(int entryIdx) {
        return entryIdx * fields;
    }


    /**
     * Returns the number of entries allocated and not deleted for this EntryArray instance.
     * Although, in case instance is used as an linked list, nextFreeIndex is can be used to calculate
     * number of entries, additional variable is used to support OakHash
     */
    int getNumOfEntries() {
        return numOfEntries.get();
    }

    /**
     * Atomically reads long field of the entries array.
     */
    protected long getEntryFieldLong(int entryIndex, int entryOffset) {
        return entries[entryIdx2LongIdx(entryIndex) + entryOffset];
    }

    protected void setEntryFieldLong(int entryIndex, int entryOffset, long value) {
        entries[entryIdx2LongIdx(entryIndex) + entryOffset] = value;
    }

    /**
     * Performs CAS of given long field of the entries longs array,
     * that should be associated with some entry.
     * CAS from 'expectedLongValue' to 'newLongValue' for field at specified offset
     *
     */
    protected boolean casEntryFieldLong(int entryIndex, int entryOffset, long expectedLongValue,
                                      long newLongValue) {
        int index = Unsafe.ARRAY_LONG_BASE_OFFSET +
                (entryIdx2LongIdx(entryIndex) + entryOffset) * Unsafe.ARRAY_LONG_INDEX_SCALE;
        return UnsafeUtils.UNSAFE.compareAndSwapLong(entries, index, expectedLongValue, newLongValue);
    }


    /********************************************************************************************/
    /*--------------- Methods for setting and getting specific key and/or value ----------------*/

    /**
     * Atomically reads the key reference from the entry (given by entry index "ei")
     */
    protected long getKeyReference(int ei) {
        return getEntryFieldLong(ei, KEY_REF_OFFSET);
    }

    /**
     * Atomically writes the key reference to the entry (given by entry index "ei")
     */
    protected void setKeyReference(int ei, long value) {
        setEntryFieldLong(ei, KEY_REF_OFFSET, value);
    }

    /**
     * casKeyReference CAS the key reference (of the entry given by entry index "ei") to be the
     * "keyRefNew" only if it was "keyRefOld". The method serves external EntryArray users.
     */
    protected boolean casKeyReference(int ei, long keyRefOld, long keyRefNew) {
        return casEntryFieldLong(ei, KEY_REF_OFFSET, keyRefOld, keyRefNew);
    }

    /**
     * Atomically reads the value reference from the entry (given by entry index "ei")
     * 8-byte align is only promised in the 64-bit JVM when allocating int arrays.
     * For long arrays, it is also promised in the 32-bit JVM.
     */
    protected long getValueReference(int ei) {
        return getEntryFieldLong(ei, VALUE_REF_OFFSET);
    }

    /**
     * casValueReference CAS the value reference (of the entry given by entry index "ei") to be the
     * "valueRefNew" only if it was "valueRefOld".
     * The method serves external EntryArray users.
     */
    protected boolean casValueReference(int ei, long valueRefOld, long valueRefNew) {
        return casEntryFieldLong(ei, VALUE_REF_OFFSET, valueRefOld, valueRefNew);
    }

    protected void copyEntriesFrom(EntryArray<K, V> other, int srcEntryIdx, int destEntryIndex, int fieldCount) {
        // ARRAY COPY: using next as the base of the entry
        // copy both the key and the value references and integer for future use => 5 integers via array copy
        // the first field in an entry is next, and it is not copied since it should be assigned elsewhere
        // therefore, to copy the rest of the entry we use the offset of next (which we assume is 0) and
        // add 1 to start the copying from the subsequent field of the entry.
        System.arraycopy(other.entries,  // source entries array
                entryIdx2LongIdx(srcEntryIdx),
                entries,                        // this entries array
                entryIdx2LongIdx(destEntryIndex), fieldCount);
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
        return valuesMemoryManager.isReferenceValidAndNotDeleted(valRef);
    }

    /**
     * Checks if a value of an entry is deleted (checks on-heap and off-heap).
     *
     * @param tempValue a reusable buffer object for internal temporary usage
     * @param ei        the entry index to check
     * @return true if the entry is deleted
     */
    boolean isValueDeleted(ValueBuffer tempValue, int ei) {
        // checking the reference,
        // it is important to check the reference first and avoid accessing off-heap if possible
        boolean isAllocatedAndNotDeleted = readValue(tempValue, ei);
        if (!isAllocatedAndNotDeleted) {
            return true;
        }
        // checking the off-heap data
        return tempValue.getSlice().isDeleted() != ValueUtils.ValueResult.FALSE;
    }

    /**
     * Checks if a key of an entry is deleted (checks on-heap and off-heap).
     *
     * @param keyBuffer a reusable buffer object for internal temporary usage
     * @param ei        the entry index to check
     * @return true if the entry is deleted
     */
    boolean isKeyDeleted(KeyBuffer keyBuffer, int ei) {
        // checking the reference,
        // it is important to check the reference first and avoid accessing off-heap if possible
        boolean isAllocatedAndNotDeleted = readKey(keyBuffer, ei);
        if (!isAllocatedAndNotDeleted) {
            return true;
        }
        // checking the off-heap data
        return keyBuffer.getSlice().isDeleted() != ValueUtils.ValueResult.FALSE;
    }

    /********************************************************************************************/
    /*- Methods for managing the read, allocation and release pathes of keys and values of a
    **  specific entry -*/
    /********************************************************************************************/

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
        if (!isIndexInBound(ei)) {
            value.invalidate();
            return false;
        }
        long reference = getValueReference(ei);
        return value.getSlice().decodeReference(reference);
    }

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
        if (!isIndexInBound(ei)) {
            key.invalidate();
            return false;
        }

        long reference = getKeyReference(ei);
        return key.getSlice().decodeReference(reference);
    }

    protected boolean isIndexInBound(int ei) {
        // The actual capacity is (entriesCapacity-1) because the first entry is a dummy.
        return (ei != INVALID_ENTRY_INDEX && ei < entriesCapacity);
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
        ctx.entryState = getValueState(ctx.value);
        assert valuesMemoryManager.isReferenceConsistent(ctx.value.getSlice().getReference());
    }

    /**
     * Find the state of a the value that is pointed by {@code value}.
     * Thus, {@code readValue(value, ei)} should be called prior to this method with the same {@code value} instance.
     *
     * @param value a buffer object that contains the value buffer
     */
    private EntryState getValueState(ValueBuffer value) {
        // value can be deleted or in the middle of being deleted
        //   remove: (1)off-heap delete bit, (2)reference deleted
        //   middle state: off-heap header marked deleted, but valid reference

        if (!valuesMemoryManager.isReferenceValid(value.getSlice().getReference())) {
            // if there is no value associated with given key,
            // thebvalue of this entry was never yet allocated
            return EntryState.UNKNOWN;
        }

        if (valuesMemoryManager.isReferenceDeleted(value.getSlice().getReference())) {
            // if value is valid the reference can still be deleted
            return  EntryState.DELETED;
        }

        // value reference is valid, just need to check if off-heap is marked deleted
        ValueUtils.ValueResult result = value.getSlice().isDeleted();

        // If result == TRUE, there is a deleted value associated with the given key
        // If result == RETRY, we ignore it, since it will be discovered later down the line as well
        return (result == ValueUtils.ValueResult.TRUE) ? EntryState.DELETED_NOT_FINALIZED : EntryState.VALID;
    }

    /**
     * Allocate and serialize (writes) a key object to off-heap KeyBuffer. Writes the key off-heap.
     *
     * @param key       the key to write
     * @param keyBuffer the off-heap KeyBuffer to update with the new allocation
     */
    void writeKey(K key, KeyBuffer keyBuffer) {
        int keySize = keySerializer.calculateSize(key);
        keyBuffer.getSlice().allocate(keySize, false);
        assert keyBuffer.isAssociated();
        ScopedWriteBuffer.serialize(keyBuffer.getSlice(), key, keySerializer);
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
    void allocateValue(ThreadContext ctx, V value, boolean writeForMove) {

        // the length of the given value plus its header
        int valueDataSize   = valueSerializer.calculateSize(value);

        // The allocated slice includes all the needed information for further access,
        // the reference is set in the slice as part of the alocation
        ctx.newValue.getSlice().allocate(valueDataSize, writeForMove);
        ctx.isNewValueForMove = writeForMove;

        ScopedWriteBuffer.serialize(ctx.newValue.getSlice(), value, valueSerializer);
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
     * Releases the key of the input context.
     * Currently in use only for unreached keys, waiting for GC to be arranged
     *
     * @param ctx the context that follows the operation since the key was found/created
     **/
    void releaseKey(ThreadContext ctx) {
        // Keys are now managed via Sequentially Expanding Memory Manager, but since this key's slice
        // can not be reached or used by other thread it is OK to release it and to allocate again.
        ctx.key.getSlice().release();
    }

    /**
     * Releases the newly allocated value of the input context.
     * Currently the method is used only to release an
     * unreachable value reference, the one that was not yet attached to an entry!
     * The method is part of EntryArray, because it cares also
     * for writing the value before attaching it to an entry (allocateValue/writeValueCommit)
     *
     * @param ctx the context that follows the operation since the key was found/created
     **/
    void releaseNewValue(ThreadContext ctx) {
        ctx.newValue.getSlice().release();
    }
}
