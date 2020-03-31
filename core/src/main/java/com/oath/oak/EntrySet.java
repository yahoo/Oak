/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;

import java.util.concurrent.atomic.AtomicInteger;


import static com.oath.oak.ValueUtils.INVALID_VERSION;
import static com.oath.oak.ValueUtils.ValueResult.FALSE;
import static com.oath.oak.ValueUtils.ValueResult.TRUE;

/* EntrySet keeps a set of entries. Entry is reference to key and value, both located off-heap.
** EntrySet provides access, updates and manipulation on each entry, provided its index.
**
** IMPORTANT: Due to limitation in amount of bits we use to represent Block ID the amount of memory
** supported by ALL OAKs in one system is 128GB only!
**
** Entry is a set of 6 (FIELDS) consecutive integers, part of "entries" int array. The definition
** of each integer is explained below. Also bits of each integer are represented in a very special
** way (also explained below). This makes any changes made to this class very delicate. Please
** update with care.
**
** Entries Array:
** --------------------------------------------------------------------------------------
** 0 | headNextIndex keeps entry index of the first ordered entry
** --------------------------------------------------------------------------------------
** 1 | NEXT  - entry index of the entry following the entry with entry index 1  |
** -----------------------------------------------------------------------------|
** 2 | KEY_POSITION           | those 2 integers together, represented as long  | entry with
** ---------------------------| provide KEY_REFERENCE. Pay attention that       | entry index
** 3 | KEY_BLOCK_AND_LENGTH   | KEY_REFERENCE is different than VALUE_REFERENCE | 1
** -----------------------------------------------------------------------------|
** 4 | VALUE_POSITION         | those 2 integers together, represented as long  | entry that
** ---------------------------| provide VALUE_REFERENCE. Pay attention that     | was allocated
** 5 | VALUE_BLOCK_AND_LENGTH | VALUE_REFERENCE is different than KEY_REFERENCE | first
 ** ----------------------------------------------------------------------------|
** 6 | VALUE_VERSION - the version of the value required for memory management  |
** --------------------------------------------------------------------------------------
** 7 | NEXT  - entry index of the entry following the entry with entry index 2  |
** -----------------------------------------------------------------------------|
** 8 | KEY_POSITION           | those 2 integers together, represented as long  | entry with
** ---------------------------| provide KEY_REFERENCE. Pay attention that       | entry index
** 9 | KEY_BLOCK_AND_LENGTH   | KEY_REFERENCE is different than VALUE_REFERENCE | 2
** -----------------------------------------------------------------------------|
** ...
**
**
** Internal class, package visibility */
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
         * KEY_REFERENCE - the blockID, length and position of the value pointed from this entry (size of two
         * integers, one long).
         *
         * KEY_POSITION
         *
         * KEY_BLOCK_AND_LENGTH - this integer holds both the blockID and the length of the key pointed by the entry.
         * Using KEY_LENGTH_MASK and KEY_BLOCK_SHIFT the blockID and length can be extracted.
         * Currently, the length of a key is limited to 32KB, and blockID is limited to 512 blocks
         * (with the current block size of 256MB, the total memory is up to 128GB).
         *
         *
         * KEY_BLOCK
         *
         * KEY_LENGTH
         *
         * VALUE_REFERENCE - the blockID, length and position of the value pointed from this entry (size of two
         * integers, one long). Equals to INVALID_VALUE_REFERENCE if no value is point.
         *
         * VALUE_POSITION
         *
         * VALUE_BLOCK_AND_LENGTH - this value holds both the blockID and the length of the value pointed by the entry.
         * Using VALUE_LENGTH_MASK and VALUE_BLOCK_SHIFT the blockID and length can be extracted.
         * Currently, the length of a value is limited to 8MB, and blockID is limited to 512 blocks
         * (with the current block size of 256MB, the total memory is up to 128GB).
         *
         * VALUE_BLOCK
         *
         * VALUE_LENGTH
         *
         *
         * VALUE_VERSION - as the name suggests this is the version of the value reference by VALUE_REFERENCE.
         * It initially equals to INVALID_VERSION.
         * If an entry with version v is removed, then this field is CASed to be -v after the value is marked
         * off-heap and the value reference becomes INVALID_VALUE.
         */
        NEXT(0), KEY_REFERENCE(1), KEY_POSITION(1), KEY_BLOCK_AND_LENGTH(2), KEY_BLOCK(2), KEY_LENGTH(2),
        VALUE_REFERENCE(3), VALUE_POSITION(3), VALUE_BLOCK_AND_LENGTH(4), VALUE_BLOCK(4), VALUE_LENGTH(4),
        VALUE_VERSION(5);

        final int value;

        OFFSET(int value) {
            this.value = value;
        }
    }

    static final long   INVALID_VALUE_REFERENCE = 0;
    static final long   INVALID_KEY_REFERENCE = 0;
    private static final int INVALID_ENTRY_INDEX = 0;
    private static final int    BLOCK_ID_LENGTH_ARRAY_INDEX = 1;
    private static final int    POSITION_ARRAY_INDEX = 0;

    // location of the first (head) node - just a next pointer (always same value 0)
    private final int headNextIndex = 0;

    // index of first item in array, after head (not necessarily first in list!)
    private static final int HEAD_NEXT_INDEX_SIZE = 1;

    private static final int FIELDS = 6;  // # of fields in each item of entries array
    // key block is part of key length integer, thus key length is limited to 65KB
    private static final int KEY_LENGTH_MASK = 0xffff; // 16 lower bits
    private static final int KEY_BLOCK_SHIFT = 16;
    // Assume the length of a value is up to 8MB because there can be up to 512 blocks
    private static final int VALUE_LENGTH_MASK = 0x7fffff;
    private static final int VALUE_BLOCK_SHIFT = 23;

    private static final Unsafe unsafe = UnsafeUtils.unsafe;
    private final MemoryManager memoryManager;

    private final int[] entries;    // array is initialized to 0 - this is important!
    private final int entriesCapacity; // number of entries (not ints) to be maximally held

    private final AtomicInteger nextFreeIndex;    // points to next free index of entry array
    // counts number of entries inserted & not deleted, pay attention that not all entries counted
    // in number of entries are finally linked into the linked list of the chunk above
    // and participating in holding the "real" KV-mappings, the "real" are counted in Chunk
    private final AtomicInteger numOfEntries;
    // for writing the keys into the off-heap bytebuffers (Blocks)
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;

    private final ValueUtils valOffHeapOperator; // is used for any value off-heap metadata access

    /*----------------- Constructor -------------------*/
    /**
     * Create a new EntrySet
     * @param memoryManager   for off-heap accesses and updates
     * @param entriesCapacity how many entries should this EntrySet keep at maximum
     * @param keySerializer   used to serialize the key when written to off-heap
     */
    EntrySet(MemoryManager memoryManager, int entriesCapacity, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer, ValueUtils valOffHeapOperator) {
        this.memoryManager   = memoryManager;
        this.entries         = new int[entriesCapacity * FIELDS + HEAD_NEXT_INDEX_SIZE];
        this.nextFreeIndex = new AtomicInteger(HEAD_NEXT_INDEX_SIZE);
        this.numOfEntries = new AtomicInteger(0);
        this.entriesCapacity = entriesCapacity;
        this.keySerializer   = keySerializer;
        this.valueSerializer = valueSerializer;
        this.valOffHeapOperator = valOffHeapOperator;
    }

    /* OpData is a class that encapsulates the EntrySet's information, from when value write started
    * and until value write was committed. It should not be used by other classes, just transferred
    * between writeValueStart (return parameter) to writeValueCommit (input parameter)
    * TODO: probably to merge with LookUp and create some operation context class */
    static class OpData {
        final int entryIndex;
        final long newValueReference;
        long oldValueReference;
        final int oldVersion;
        final int newVersion;
        final Slice slice;

        OpData(int entryIndex, long oldValueReference, long newValueReference,
            int oldVersion, int newVersion, Slice slice) {
            this.entryIndex = entryIndex;
            this.newValueReference = newValueReference;
            this.oldValueReference = oldValueReference;
            this.oldVersion = oldVersion;
            this.newVersion = newVersion;
            this.slice = slice;
        }
    }

    /* LookUp is a class that encapsulates the EntrySet's information, from when entry (key) was
     * (or wasn't) found and until the operation was completed.
     * It should not be used by other classes, just transferred.
     * TODO: probably to merge with OpData and create some single operation context class */
    static class LookUp {
        /**
         * valueSlice is used for easier access to the off-heap memory. The location pointed by it
         * is the one referenced by valueReference.
         * If it is {@code null} it means the value is deleted or marked as deleted but still referenced by
         * valueReference.
         */
        Slice valueSlice;
        /**
         * Set to true when entry is found deleted, but not yet suitable to be reused.
         * Deletion consists of 3 steps: (1) mark off-heap deleted (LP),
         * (2) CAS value reference to invalid, (3) CAS value version to negative.
         * If not all three steps are done entry can not be reused for new insertion.
         */
        boolean isFinalizeDeletionNeeded;
        /**
         * valueReference is composed of 3 numbers: block ID, value position and value length. All these numbers are
         * squashed together into a long using the VALUE masks, shifts and indices.
         * When it equals to {@code INVALID_VALUE_REFERENCE} is means that there is no value referenced from entryIndex.
         * This field is usually used for CAS purposes since it sits in each entry.
         */
        final long valueReference;
        int entryIndex;
        /**
         * This is the version of the value referenced by {@code valueReference}.
         * If {@code valueReference == INVALID_VALUE_REFERENCE}, then:
         * {@code version <= INVALID_VERSION} if the removal was completed.
         * {@code version > INVALID_VERSION} if the removal was not completed.
         * else
         * {@code version <= INVALID_VERSION} if the insertion was not completed.
         * {@code version > INVALID_VERSION} if the insertion was completed.
         */
        int version;
        /* key reference, build differently than value reference */
        final long keyReference;

        LookUp(Slice valueSlice, long valueReference, int entryIndex, int version,
            long keyReference, boolean isFinalizeDeleteNeeded) {
            this.valueSlice = valueSlice;
            this.valueReference = valueReference;
            this.entryIndex = entryIndex;
            this.version = version;
            this.keyReference = keyReference;
            this.isFinalizeDeletionNeeded = isFinalizeDeleteNeeded;
        }
    }


    /********************************************************************************************/
    /*---------------- Methods for setting and getting specific entry's field ------------------*/
    // we use term intIdx for the integer's index inside the entries array (referred as a set of integers),
    // we use term entryIdx for the index of entry array (referred as a set of entries)

    private int entryIdx2intIdx(int entryIdx) {
        if (entryIdx == 0) { // assumed it is a head pointer access
            return 0;
        }
        return ((entryIdx-1) * FIELDS) + HEAD_NEXT_INDEX_SIZE;
    }

    /**
     * getEntryArrayFieldInt gets the integer field of entry at specified offset for given
     * start of the entry index in the entry array.
     * The field is read atomically as it is a machine word, however the concurrency of the
     * mostly updated value is not ensured as no memory fence is issued.
     */
    private int getEntryArrayFieldInt(int intFieldIdx, OFFSET offset) {
        switch (offset) {
        case KEY_LENGTH:
            // return two low bytes of the key length index int
            return entries[intFieldIdx + offset.value] & KEY_LENGTH_MASK;
        case KEY_BLOCK:
            // offset must be OFFSET_KEY_BLOCK, return 2 high bytes of the int inside key length
            // right-shift force, fill empty with zeroes
            return entries[intFieldIdx + offset.value] >>> KEY_BLOCK_SHIFT;
        case VALUE_LENGTH:
            return entries[intFieldIdx + offset.value] & VALUE_LENGTH_MASK;
        case VALUE_BLOCK:
            return entries[intFieldIdx + offset.value] >>> VALUE_BLOCK_SHIFT;
        default:
            return entries[intFieldIdx + offset.value]; // used for NEXT and KEY/VALUE_POSITION
        }
    }

    /**
     * getEntryArrayFieldLong atomically reads two integers field of the entries array.
     * Should be used with OFFSET.VALUE_REFERENCE and OFFSET.KEY_REFERENCE
     * The concurrency is ensured due to memory fence as part of "volatile"
     */
    private long getEntryArrayFieldLong(int intStartFieldIdx, OFFSET offset) {
        assert offset == OFFSET.VALUE_REFERENCE || offset == OFFSET.KEY_REFERENCE;
        long arrayOffset =
            Unsafe.ARRAY_INT_BASE_OFFSET + (intStartFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE;
        assert arrayOffset % 8 == 0;
        return unsafe.getLongVolatile(entries, arrayOffset);
    }

    /**
     * setEntryFieldInt sets the integer field of specified offset to 'value'
     * for given integer index in the entry array
     */
    private void setEntryFieldInt(int intFieldIdx, OFFSET offset, int value) {
        assert intFieldIdx + offset.value >= 0;
        switch (offset) {
        case KEY_LENGTH:
            // OFFSET_KEY_LENGTH and OFFSET_KEY_BLOCK should be less then 16 bits long
            // *2 in order to get read of the signed vs unsigned limits
            assert value < Short.MAX_VALUE * 2;
            // set two low bytes of the key block id and length index
            entries[intFieldIdx + offset.value] |= (value & KEY_LENGTH_MASK);
            return;
        case KEY_BLOCK:
            // OFFSET_KEY_LENGTH and OFFSET_KEY_BLOCK should be less then 16 bits long
            // *2 in order to get read of the signed vs unsigned limits
            assert value < Short.MAX_VALUE * 2;
            // offset must be OFFSET_KEY_BLOCK,
            // set 2 high bytes of the int inside OFFSET_KEY_LENGTH
            assert value > 0; // block id can never be 0
            entries[intFieldIdx + offset.value] |= (value << KEY_BLOCK_SHIFT);
            return;
        case VALUE_LENGTH:
            // make sure the length is at most 2^23 and at least 0
            assert (value & VALUE_LENGTH_MASK) == value;
            entries[intFieldIdx + offset.value] |= (value & VALUE_LENGTH_MASK);
            return;
        case VALUE_BLOCK:
            assert value > 0; // block id can never be 0
            assert ((value << VALUE_BLOCK_SHIFT) >>> VALUE_BLOCK_SHIFT) == value; // value is up to 2^9
            entries[intFieldIdx + offset.value] |= (value << VALUE_BLOCK_SHIFT);
            return;
        default:
            entries[intFieldIdx + offset.value] = value; // used for NEXT
        }
    }

    /**
     * setEntryFieldLong sets the two integers field of specified offset to 'value'
     * for given integer index in the entry array
     */
    private void setEntryFieldLong(int item, OFFSET offset, long value) {
        long arrayOffset = Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE;
        assert arrayOffset % 8 == 0;
        unsafe.putLongVolatile(entries, arrayOffset, value);
    }

    /**
     * casEntriesArrayInt performs CAS of given integer field of the entries ints array,
     * that should be associated with some field of some entry.
     * CAS from 'expectedIntValue' to 'newIntValue' for field at specified offset
     * of given int-field in the entries array
     */
    private boolean casEntriesArrayInt(
        int intFieldIdx, OFFSET offset, int expectedIntValue, int newIntValue) {
        return unsafe.compareAndSwapInt(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (intFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expectedIntValue, newIntValue);
    }

    /**
     * casEntriesArrayLong performs CAS of given two integers field of the entries ints array,
     * that should be associated with some 2 consecutive integer fields of some entry.
     * CAS from 'expectedLongValue' to 'newLongValue' for field at specified offset
     * of given int-field in the entries array
     */
    private boolean casEntriesArrayLong(
        int intStartFieldIdx, OFFSET offset, long expectedLongValue, long newLongValue) {
        return unsafe.compareAndSwapLong(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (intStartFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expectedLongValue, newLongValue);
    }

    /********************************************************************************************/
    /*--------------- Methods for setting and getting specific key and/or value ----------------*/
    // key or value references are the triple <blockID, position, length> encapsulated in long
    // whenever version is needed it is an additional integer

    /**
     * Atomically reads the value version from the entry (given by entry index "ei")
     * synchronisation with reading the value reference is not ensured in this method
     * */
    private int getValueVersion(int ei) {
        return getEntryArrayFieldInt(entryIdx2intIdx(ei), OFFSET.VALUE_VERSION);
    }

    /**
     * Atomically reads the value reference from the entry (given by entry index "ei")
     * synchronisation with reading the value version is not ensured in this method
     * */
    private long getValueReference(int ei) {
        return getEntryArrayFieldLong(entryIdx2intIdx(ei), OFFSET.VALUE_REFERENCE);
    }

    /**
     * Atomically reads both the value reference and its value (comparing the version).
     * It does that by using the atomic snapshot technique (reading the version, then the reference,
     * and finally the version again, checking that it matches the version read previously).
     * Since a snapshot is used, the LP is when reading the value reference, if the versions match,
     * otherwise, the operation restarts.
     *
     * @param ei The entry of which the reference and version are read
     * @param version    an output parameter to return the version
     * @return the read value reference
     */
    private long getValueReferenceAndVersion(int ei, int[] version) {
        long valueReference;
        int v;
        do {
            v = getValueVersion(ei);
            valueReference = getValueReference(ei);
        } while (v != getValueVersion(ei));
        version[0] = v;
        return valueReference;
    }

    /**
     * Atomically reads the key reference from the entry (given by entry index "ei")
     */
    private long getKeyReference(int ei) {
        return getEntryArrayFieldLong(entryIdx2intIdx(ei), OFFSET.KEY_REFERENCE);
    }

    /**
     * Atomically reads the key reference from the entry (given by entry index "ei")
     * and assigns blockID, position and length into given int array
     */
    private int[] getKeyReference2IntArray(int ei) {
        long keyReference = getKeyReference(ei);
        int[] answer = new int[3];
        int[] keyArray = UnsafeUtils.longToInts(keyReference);
        answer[0] = getKeyBlockIDFromIntArray(keyArray);   // key's BlockID
        answer[1] = getPositionFromIntArray(keyArray);  // key's position
        answer[2] = getKeyLengthFromIntArray(keyArray);  // key's length
        return answer;
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
    /*--------------- Methods for helping managing off-heap references ----------------*/
    // key or value references are the triple <blockID, position, length> encapsulated in long
    // here we translate them (back and forth) to Slice or integers
    // TODO: not directly related to EntrySet consider making static or moving to some utils

    /**
     * buildValueSlice builds a slice given value reference
     */
    static Slice buildValueSlice(long valueReference, int version, MemoryManager mm) {
        if (valueReference == INVALID_VALUE_REFERENCE) {
            return null;
        }
        int[] valueArray = UnsafeUtils.longToInts(valueReference);
        Slice s = new Slice(
            getValueBlockIDFromIntArray(valueArray),
            getPositionFromIntArray(valueArray),
            getValueLengthFromIntArray(valueArray),
            mm);
        s.setVersion(version);
        return s;
    }

    /**
     * getValuePosition returns value position from the value reference (service method)
     */
    static int getValuePosition(long valueReference) {
        int[] valueArray = UnsafeUtils.longToInts(valueReference);
        return getPositionFromIntArray(valueArray);
    }

    /**
     * getValueLength returns value length from the value reference (service method)
     */
    static int getValueLength(long valueReference) {
        int[] valueArray = UnsafeUtils.longToInts(valueReference);
        return getValueLengthFromIntArray(valueArray);
    }

    /**
     * buildValueSlice builds a value slice given entry index
     */
    Slice buildValueSlice(int ei) {

        int[] version = new int[1]; // the array of size 1 in order to get output inside it
        // Atomic snapshot of version and value reference
        long valueReference = getValueReferenceAndVersion(ei, version);
        return buildValueSlice(valueReference, version[0], memoryManager);
    }

    /**
     * buildKeySlice builds a key slice given entry index
     */
    private Slice buildKeySlice(int ei) {
        long keyReference = getKeyReference(ei);
        assert keyReference != INVALID_KEY_REFERENCE;
        int[] keyArray = UnsafeUtils.longToInts(keyReference);
        int blockID = getKeyBlockIDFromIntArray(keyArray);
        int keyPosition = getPositionFromIntArray(keyArray);
        int length = getKeyLengthFromIntArray(keyArray);
        return new Slice(blockID, keyPosition, length, memoryManager);
    }

    /**
     * buildValueReference builds a long reference given a slice
     */
    private long buildValueReference(Slice slice, int valueLength) {
        int valueBlockAndLength = (slice.getBlockID() << VALUE_BLOCK_SHIFT) | (valueLength & VALUE_LENGTH_MASK);
        return UnsafeUtils.intsToLong(slice.getOriginalPosition(), valueBlockAndLength);
    }

    /********************************************************************************************/
    /*--------------- Methods for helping managing off-heap references ----------------*/
    // key or value references are the triple <blockID, position, length> encapsulated in long

    /**
     * getKeyBlockIDFromIntArray extracts keys BlockID from the translation of the key reference (long)
     * into array of two integers. This is done in order to avoid more int[] objects.
     *
     * Always use >>> (unsigned right shift) operator. It always fills 0 irrespective
     * of the sign of the number.
     */
    static private int getKeyBlockIDFromIntArray(int[] long2IntTranslated) {
        return long2IntTranslated[BLOCK_ID_LENGTH_ARRAY_INDEX] >>> KEY_BLOCK_SHIFT;
    }

    /**
     * getValueBlockIDFromIntArray extracts value BlockID from the translation of the value reference (long)
     * into array of two integers. This is done in order to avoid more int[] objects.
     *
     * Always use >>> (unsigned right shift) operator. It always fills 0 irrespective
     * of the sign of the number.
     */
    static private int getValueBlockIDFromIntArray(int[] long2IntTranslated) {
        return long2IntTranslated[BLOCK_ID_LENGTH_ARRAY_INDEX] >>> VALUE_BLOCK_SHIFT;
    }

    /**
     * getPositionFromIntArray extracts in Block position from the translation of the off-heap
     * reference (long) into array of two integers. This is done in order to limit int[] objects.
     */
    static private int getPositionFromIntArray(int[] long2IntTranslated) {
        return long2IntTranslated[POSITION_ARRAY_INDEX];
    }

    /**
     * getKeyLengthFromIntArray extracts keys length from the translation of the key reference (long)
     * into array of two integers. This is done in order to avoid more int[] objects.
     */
    static private int getKeyLengthFromIntArray(int[] long2IntTranslated) {
        return long2IntTranslated[BLOCK_ID_LENGTH_ARRAY_INDEX] & KEY_LENGTH_MASK;
    }

    /**
     * getValueLengthFromIntArray extracts value length from the translation of the value reference (long)
     * into array of two integers. This is done in order to avoid more int[] objects.
     */
    static private int getValueLengthFromIntArray(int[] long2IntTranslated) {
        return long2IntTranslated[BLOCK_ID_LENGTH_ARRAY_INDEX] & VALUE_LENGTH_MASK;
    }

    /********************************************************************************************/
    /*-------------- Methods for managing keys and values of a specific entry' -----------------*/
    // Memory Manager interface is via Slice, although Slices are not key/value containers in the
    // EntrySet. MM allocates/releases Slices

    /**
     * buildLookUp builds a LookUp from an entry given by entry index "ei",
     * so this entry can be referred to later
     *
     * If the entry itself or off-heap value is deleted the returned LookUp has null as a Slice
     */
    LookUp buildLookUp(int ei) {
        int[] version = new int[1]; // the array of size 1 in order to get output inside it
        // Atomic snapshot of version and value reference
        long valueReference = getValueReferenceAndVersion(ei, version);

        Slice valueSlice = buildValueSlice(valueReference, version[0], memoryManager);
        if (valueSlice == null) {
            // There is no value associated with the given key
            assert valueReference == INVALID_VALUE_REFERENCE;
            // we can be in the middle of insertion or in the middle of removal
            // insert: (1)reference+version=invalid, (2)reference set, (3)version set
            //          middle state valid reference, but invalid version
            // remove: (1)off-heap delete bit, (2)reference invalid, (3)version negative
            //          middle state invalid reference, valid version
            return new LookUp(
                null, valueReference, ei, version[0], getKeyReference(ei),
                (version[0] >= INVALID_VERSION)); // if version is negative no need to finalize delete
        }
        ValueUtils.ValueResult result = valOffHeapOperator.isValueDeleted(valueSlice, valueSlice.getVersion());
        if (result == TRUE) {
            // There is a deleted value associated with the given key
            return new LookUp(
                null, valueReference, ei, valueSlice.getVersion(), getKeyReference(ei),
                true);
        }
        // If result == RETRY, we ignore it, since it will be discovered later down the line as well
        return new LookUp(valueSlice, valueReference, ei, valueSlice.getVersion(), getKeyReference(ei), false);
    }

    /**
     * writeKey writes given key object "key" (to off-heap) as a serialized key, referenced by entry
     * at the entries index "ei"
     **/
    private void writeKey(K key, int ei) {
        int keySize = keySerializer.calculateSize(key);
        int intIdx = entryIdx2intIdx(ei);
        Slice s = memoryManager.allocateSlice(keySize, MemoryManager.Allocate.KEY);
        // byteBuffer.slice() is set so it protects us from the overwrites of the serializer
        // TODO: better serializer need to be given OakWBuffer and not ByteBuffer
        keySerializer.serialize(key, s.getByteBuffer().slice());

        setEntryFieldInt(intIdx, OFFSET.KEY_BLOCK, s.getBlockID());
        setEntryFieldInt(intIdx, OFFSET.KEY_POSITION, s.getOriginalPosition());
        setEntryFieldInt(intIdx, OFFSET.KEY_LENGTH, keySize);
    }

    /**
     * readKey reads a key from entry at the given entry index (from off-heap).
     * Key is returned via reusable thread-local ByteBuffer.
     * There is no copy just a special ByteBuffer for a single key.
     * The thread-local ByteBuffer can be reused by different threads, however as long as
     * a thread is invoked the ByteBuffer is related solely to this thread.
     */
    ByteBuffer readKey(int ei) {
        if (ei == INVALID_ENTRY_INDEX) {
            return null;
        }

        long keyReference = getKeyReference(ei);
        return keyRefToByteBuffer(keyReference, memoryManager);
    }

    static ByteBuffer keyRefToByteBuffer(long keyRef, MemoryManager mm) {
        int[] keyArray = UnsafeUtils.longToInts(keyRef);
        int blockID = getKeyBlockIDFromIntArray(keyArray);
        int keyPosition = getPositionFromIntArray(keyArray);
        int length = getKeyLengthFromIntArray(keyArray);

        return mm.getByteBufferFromBlockID(blockID, keyPosition, length);
    }

    void setKeyOutputRBuff(int ei, OakRKeyBuffer keyRef) {
        if (ei == INVALID_ENTRY_INDEX) {
            return;
        }
        long keyReference = getKeyReference(ei);
        keyRefToOakRRef(keyReference, keyRef);
    }

    /**
     * Sets the given external key reference (OakRReference) given the long key reference.
     * Used while iterating, when lookUp context is not used.
     * There is no copy just a special ByteBuffer for a single key.
     * The thread-local ByteBuffer can be reused by different threads, however as long as
     * a thread is invoked the ByteBuffer is related solely to this thread.
     */
    static void keyRefToOakRRef(long keyReference, OakRKeyBuffer oakKeyRef) {
        int[] keyArray = UnsafeUtils.longToInts(keyReference);
        int blockID = getKeyBlockIDFromIntArray(keyArray);
        int keyPosition = getPositionFromIntArray(keyArray);
        int length = getKeyLengthFromIntArray(keyArray);

        oakKeyRef.setReference(blockID, keyPosition, length);
    }

    /**
     * Sets the given value reference (OakRReference) given the entry index.
     * Returns false if: (1)there is no such entry or (2)entry has no value set
     * Returns true otherwise
     * a thread is invoked on the ByteBuffer is related solely to this thread.
     */
    boolean setValueOutputRBuff(int ei, OakRKeyBuffer valueRef) {
        if (ei == INVALID_ENTRY_INDEX) {
            return false;
        }
        long valueReference = getValueReference(ei);
        if (valueReference == INVALID_VALUE_REFERENCE) {
            return false;
        }
        int[] valueArray = UnsafeUtils.longToInts(valueReference);
        int blockID = getValueBlockIDFromIntArray(valueArray);
        int valuePosition = getPositionFromIntArray(valueArray);
        int length = getValueLengthFromIntArray(valueArray);

        valueRef.setReference(blockID, valuePosition, length);
        return true;
    }

    /**
     * releaseKey releases key in slice, currently in use only for unreached keys,
     * waiting for GC to be arranged
     *
     * @param lookUp*/
    void releaseKey(LookUp lookUp) {
        memoryManager.releaseSlice(buildKeySlice(lookUp.entryIndex));
    }

    /**
     * releaseValue releases value in slice, currently the method is used only to release an
     * unreachable value reference, the one that was not yet attached to an entry!
     * The method is part of EntrySet, because it cares also
     * for writing the value before attaching it to an entry (writeValueStart/writeValueCommit)
     **/
    void releaseValue(OpData opData) {
        memoryManager.releaseSlice(
            buildValueSlice(opData.newValueReference, INVALID_VERSION, memoryManager));
    }

    /**
     * writeValueStart writes value off-heap. Supposed to be for entry index "ei",
     * but this entry metadata is not updated in this method. This is an intermediate step in
     * the process of inserting key-value pair, it will be finished with writeValueCommit.
     * The off-heap header is initialized in this function as well.
     *
     * @param lookUp the structure that follows the operation since the key being found.
     *               Needed here for the entry index (of the entry to be updated
     *               when insertion of this key-value pair will be committed) and for the old version.
     * @param value the value to write off-heap
     * @param writeForMove
     * @return OpData to be used later in the writeValueCommit
     **/
    OpData writeValueStart(LookUp lookUp, V value, boolean writeForMove) {
        // the length of the given value plus its header
        int valueLength = valueSerializer.calculateSize(value) + valOffHeapOperator.getHeaderSize();

        // The allocated slice is actually the thread's ByteBuffer moved to point to the newly
        // allocated slice. Version in time of allocation is set as part of the slice data.
        Slice slice = memoryManager.allocateSlice(valueLength, MemoryManager.Allocate.VALUE);

        // for value written for the first time:
        // initializing the off-heap header (version and the lock to be free)
        // for value being moved, initialize the lock to be locked
        if (writeForMove) {
            valOffHeapOperator.initLockedHeader(slice);
        }
        else { valOffHeapOperator.initHeader(slice); }

        // since this is a private environment, we can only use ByteBuffer::slice, instead of ByteBuffer::duplicate
        // and then ByteBuffer::slice
        // To be safe we create a new ByteBuffer object (for the serializer).
        valueSerializer.serialize(value, valOffHeapOperator.getValueByteBufferNoHeaderPrivate(slice));

        // combines the blockID with the value's length (including the header)
        long valueReference = buildValueReference(slice, valueLength);

        return new OpData(lookUp.entryIndex,
            writeForMove ? lookUp.valueReference : INVALID_VALUE_REFERENCE,
            valueReference, lookUp.version, slice.getVersion(), slice);
    }

    /**
     * writeValueCommit does the physical CAS of the value reference, which is the Linearization
     * Point of the insertion. It then tries to complete the insertion by CASing the value's version
     * if was not yet assigned (@see #writeValueFinish(LookUp)).
     *
     * @param opData - holds the entry to which the value reference is linked, the old and new value
     *                references and the old and new value versions.
     * @param changeForMove
     * @return {@code true} if the value reference was CASed successfully.
     */
    ValueUtils.ValueResult writeValueCommit(OpData opData, boolean changeForMove) {

        if (!changeForMove) assert opData.oldValueReference == INVALID_VALUE_REFERENCE;
        assert opData.newValueReference != INVALID_VALUE_REFERENCE;
        int intIdx = entryIdx2intIdx(opData.entryIndex);
        if (!casEntriesArrayLong(intIdx, OFFSET.VALUE_REFERENCE,
            opData.oldValueReference, opData.newValueReference)) {
            return FALSE;
        }
        casEntriesArrayInt(intIdx, OFFSET.VALUE_VERSION, opData.oldVersion, opData.newVersion);
        return TRUE;
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

    /**
     * isDeleteValueFinishNeeded checks whether the version in the given lookUp is negative
     * (which means deleted) OR INVALID. Additionally check the off-heap deleted bit.
     * We can not proceed on entry with negative version,
     * it is first needs to be changed to invalid, then any other value reference (with version)
     * can be assigned to this entry (same key).
     */
    boolean isDeleteValueFinishNeeded(LookUp lookUp){
        return lookUp.isFinalizeDeletionNeeded;
    }

    /**
     * writeValueFinish completes the insertion of a value to Oak. When inserting a value, the value
     * reference is CASed inside the entry and only then the version is CASed. Thus, there can be a
     * time in which the entry's value version is INVALID_VERSION or a negative one. In this method,
     * the version is CASed to complete the insertion.
     *
     * writeValueFinish is used in cases when in an entry the value reference and its off-heap and
     * on-heap versions do not match. In this case it is assumed that we are
     * in the middle of committing a value write and need to write the off-heap value on-heap.
     *
     * <p>
     * The version written to entry is the version written in the off-heap memory. There is no worry
     * of concurrent removals since these removals will have to first call this function as well,
     * and they eventually change the version as well.
     *
     * @param lookUp - It holds the entry to CAS, the previously written version of this entry
     *               and the value reference from which the correct version is read.
     * @return a version is returned.
     *
     * If returned value (version) is {@code INVALID_VERSION} it means that a CAS was not preformed.
     * Otherwise, a positive version is returned, and it is the version written to the entry
     * (maybe by some other thread).
     * <p>
     * Note 1: the version in the input param {@code lookUp} is updated in this method to be the
     * updated one if a valid version was returned.
     *
     * Note 2: updating of the entries MUST be under published operation. The invoker of this method
     * is responsible to call it inside the publish/unpublish scope.
     */
    int writeValueFinish(LookUp lookUp) { //TODO: check how not to return the version
        int entryVersion = lookUp.version;

        if (entryVersion > INVALID_VERSION) { // no need to complete a thing
            return entryVersion;
        }

        Slice valueSlice = buildValueSlice(lookUp.valueReference, entryVersion, memoryManager);
        int offHeapVersion = valOffHeapOperator.getOffHeapVersion(valueSlice);
        casEntriesArrayInt(entryIdx2intIdx(lookUp.entryIndex), OFFSET.VALUE_VERSION,
            entryVersion, offHeapVersion);
        lookUp.version = offHeapVersion;
        return offHeapVersion;
    }

    /**
     * deleteValueFinish completes the deletion of a value in Oak, by marking the value reference in
     * entry, after the on-heap value was already marked as deleted.
     *
     * As written in {@code writeValueFinish(LookUp)}, when updating an entry, the value reference
     * is CASed first and later the value version, and the same applies when removing a value.
     * However, there is another step before deleting an entry (remove a value), it is marking
     * the value off-heap (the LP).
     *
     * deleteValueFinish is used to first CAS the value reference to {@code INVALID_VALUE_REFERENCE}
     * and then CAS the version to be a negative one. Other threads seeing a value marked as deleted
     * call this function before they proceed (e.g., before performing a successful {@code putIfAbsent}).
     *
     * @param lookUp - holds the entry to change, the old value reference to CAS out, and the current value version.
     * @return true if the deletion indeed updated the entry to be deleted as a unique operation
     *
     * Note: updating of the entries MUST be under published operation. The invoker of this method
     * is responsible to call it inside the publish/unpublish scope.
     */
    boolean deleteValueFinish(LookUp lookUp) {
        int version = lookUp.version;
        if (version <= INVALID_VERSION) { // version is marked deleted
            return false;
        }
        int indIdx = entryIdx2intIdx(lookUp.entryIndex);
        // Scenario: this value space is allocated once again and assigned into the same entry,
        // while this thread is sleeping. So later a valid value reference is CASed to invalid.
        // In order to not allow this scenario happen we must release the
        // value's off-heap slice to memory manager only after deleteValueFinish is called.
        casEntriesArrayLong(indIdx, OFFSET.VALUE_REFERENCE,
            lookUp.valueReference, INVALID_VALUE_REFERENCE);
        if (casEntriesArrayInt(indIdx, OFFSET.VALUE_VERSION, version, -version)) {
            numOfEntries.getAndDecrement();
            // release the slice
            if (lookUp.valueSlice != null) {
                memoryManager.releaseSlice(lookUp.valueSlice);
            }
            return true;
        }
        return false;
    }

    /**
     * allocateEntry creates/allocates an entry for the key. An entry is always associated with a key,
     * therefore the key is written to off-heap and associated with the entry simultaneously.
     * The value of the new entry is set to NULL: <INVALID_VALUE_REFERENCE, INVALID_VERSION></>
     **/
    LookUp allocateEntry(K key) {
        int ei = nextFreeIndex.getAndIncrement();
        if (ei > entriesCapacity) {
            return null;
        }
        numOfEntries.getAndIncrement();
        int intIdx = entryIdx2intIdx(ei);
        // key and value must be set before returned
        // setting the value reference to <INVALID_VALUE_REFERENCE, INVALID_VERSION>
        // TODO: should we do the following setting? Because it is to set zero on zero...
        setEntryFieldLong(intIdx, OFFSET.VALUE_REFERENCE, INVALID_VALUE_REFERENCE);
        setEntryFieldInt(intIdx, OFFSET.VALUE_VERSION, INVALID_VERSION);

        writeKey(key, ei);
        return new
            LookUp(null,INVALID_VALUE_REFERENCE,ei,INVALID_VERSION,getKeyReference(ei), false);
    }

    boolean isEntryDeleted(int ei) {
        int[] valueVersion = new int[1];
        long valueReference = getValueReferenceAndVersion(ei, valueVersion);
        return (valueReference == INVALID_VALUE_REFERENCE) ||
            valOffHeapOperator.isValueDeleted(
                buildValueSlice(valueReference, valueVersion[0], memoryManager), valueVersion[0]) != FALSE;
    }

    /*
     * isValueRefValid is used only to check whether the value reference, which is part of the
     * entry on entry index "ei" is valid. No version check and no off-heap value deletion mark check.
     * Negative version is not checked, because negative version assignment will follow the
     * invalid reference assignment.ass
     * Pay attention that value may be deleted (value reference marked invalid) asynchronously
     * by other thread just after this check. For the thread safety use a copy of value reference (next method.)
     * */
    boolean isValueRefValid(int ei) {
        return getValueReference(ei) != INVALID_VALUE_REFERENCE;
    }

    /*
    * When value is connected to entry, first the value reference is CASed to the new one and after
    * the value version is set to the new one (written off-heap). Inside entry, when value reference
    * is invalid its version can only be invalid (0) or negative. When value reference is valid and
    * its version is either invalid (0) or negative, the insertion or deletion of the entry wasn't
    * accomplished, and needs to be accomplished.
    * */
    boolean isValueLinkFinished(LookUp lookUp) {
        return !((lookUp.valueReference != INVALID_VALUE_REFERENCE) &&
                 (lookUp.version <= INVALID_VERSION));
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
     *
     * The next pointers of the entries are requested to be set by the user if needed.
     *
     * @return false when this EntrySet is full
     *
     * NOT THREAD SAFE
     * */
    boolean copyEntry(EntrySet<K,V> srcEntrySet, int srcEntryIdx) {
        if (srcEntryIdx == headNextIndex) {
            return false;
        }

        // don't increase the nextFreeIndex yet, as the source entry might not be copies
        int destEntryIndex = nextFreeIndex.get();

        if (destEntryIndex > entriesCapacity) {return false;}

        if (srcEntrySet.isEntryDeleted(srcEntryIdx)) {return true;}

        // ARRAY COPY: using next as the base of the entry
        // copy both the key and the value references the value's version => 5 integers via array copy
        // the first field in an entry is next, and it is not copied since it should be assigned elsewhere
        // therefore, to copy the rest of the entry we use the offset of next (which we assume is 0) and
        // add 1 to start the copying from the subsequent field of the entry.
        System.arraycopy(srcEntrySet.entries,  // source entries array
            entryIdx2intIdx(srcEntryIdx) + OFFSET.NEXT.value + 1,
            entries,                        // this entries array
            entryIdx2intIdx(destEntryIndex) + OFFSET.NEXT.value + 1, (FIELDS - 1));

        // now it is the time to increase nextFreeIndex
        nextFreeIndex.getAndIncrement();
        numOfEntries.getAndIncrement();
        return true;
    }


}
