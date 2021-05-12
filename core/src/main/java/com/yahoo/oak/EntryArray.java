/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

public class EntryArray<K, V> {
    /***
     * KEY_REFERENCE - the blockID, length and offset of the value pointed from this entry (size of two
     * integers, one long).
     *
     * VALUE_REFERENCE - the blockID, offset and version of the value pointed from this entry (size of two
     * integers, one long). Equals to INVALID_REFERENCE if no value is point.
     */
    protected final static int KEY_REFERENCE = 0;
    protected final static int VALUE_REFERENCE = 1;

    final MemoryManager valuesMemoryManager;
    final MemoryManager keysMemoryManager;
    private final long[] entries;    // array is initialized to 0 - this is important!
    private final int fields;  // # of primitive fields in each item of entries array

    protected final int entriesCapacity; // number of entries (not ints) to be maximally held

    // counts number of entries inserted & not deleted, pay attention that not all entries counted
    // in number of entries are finally linked into the linked list of the chunk above
    // and participating in holding the "real" KV-mappings, the "real" are counted in Chunk
    protected final AtomicInteger numOfEntries;

    // for writing the keys into the off-heap bytebuffers (Blocks)
    final OakSerializer<K> keySerializer;
    final OakSerializer<V> valueSerializer;

    /**
     * Create a new EntrySet
     * @param vMM   for values off-heap allocations and releases
     * @param kMM off-heap allocations and releases for keys
     * @param entriesCapacity how many entries should this EntrySet keep at maximum
     * @param keySerializer   used to serialize the key when written to off-heap
     */
    EntryArray(MemoryManager vMM, MemoryManager kMM, int additionalFieldCount, int entriesCapacity,
               OakSerializer<K> keySerializer,
             OakSerializer<V> valueSerializer) {
        this.valuesMemoryManager = vMM;
        this.keysMemoryManager = kMM;
        this.fields = additionalFieldCount + 2;
        this.entries = new long[entriesCapacity * this.fields];
        this.numOfEntries = new AtomicInteger(0);
        this.entriesCapacity = entriesCapacity;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
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
     * Returns the number of entries allocated and not deleted for this EntrySet.
     * Although, in case EntrySet is used as an array, nextFreeIndex is can be used to calculate
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
        return getEntryFieldLong(ei, KEY_REFERENCE);
    }

    /**
     * Atomically writes the key reference to the entry (given by entry index "ei")
     */
    protected void setKeyReference(int ei, long value) {
        setEntryFieldLong(ei, KEY_REFERENCE, value);
    }

    /**
     * Atomically reads the value reference from the entry (given by entry index "ei")
     * 8-byte align is only promised in the 64-bit JVM when allocating int arrays.
     * For long arrays, it is also promised in the 32-bit JVM.
     */
    protected long getValueReference(int ei) {
        return getEntryFieldLong(ei, VALUE_REFERENCE);
    }

    /**
     * casNextEntryIndex CAS the next entry index (of the entry given by entry index "ei") to be the
     * "nextNew" only if it was "nextOld". Input parameter "nextNew" must be a valid entry index.
     * The method serves external EntrySet users.
     */
    protected boolean casValueReference(int ei, long nextOld, long nextNew) {
        return casEntryFieldLong(ei, VALUE_REFERENCE, nextOld, nextNew);
    }

    protected void copyEntryFrom(EntryArray<K, V> other, int srcEntryIdx, int destEntryIndex, int fieldCount) {
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

}
