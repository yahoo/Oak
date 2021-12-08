/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

interface EntryArrayInterface {
    /**
     * offset in primitive fields of one entry, to a reference as coded by
     * keysMemoryManager, of the key pointed from this entry (size of long)
     */
    int KEY_REF_OFFSET = 0;

    /**
     * offset in primitive fields of one entry, to a reference as coded by
     * valuesMemoryManager, of the value pointed from this entry (size of long).
     * The primitive field equals to INVALID_REFERENCE if no value is point.
     */
    int VALUE_REF_OFFSET = 1;

    int INVALID_ENTRY_INDEX = -1;

    /**
     * @return the number of entries in the array
     */
    int entryCount();

    /**
     * @return the number of fields (64bit) in each entry
     */
    int fieldCount();

    /**
     * Brings the array to its initial state with all zeroes.
     * Used when we want to empty the structure without reallocating all the objects/memory
     * NOT THREAD SAFE !!!
     */
    void clear();

    /**
     * Atomically reads long field of an entry.
     * @param entryIndex the index of the entry
     * @param fieldIndex the index of the field inside the entry
     * @return the field's value
     */
    long getEntryFieldLong(int entryIndex, int fieldIndex);

    /**
     * Atomically sets long field of an entry.
     * @param entryIndex the index of the entry
     * @param fieldIndex the index of the field inside the entry
     * @param value the field's new value
     */
    void setEntryFieldLong(int entryIndex, int fieldIndex, long value);

    /**
     * Performs CAS of given long field of an entry.
     * @param entryIndex the index of the entry
     * @param fieldIndex the index of the field inside the entry
     * @param expectedValue the field's expected current value
     * @param newValue the field's new value
     * @return true if successful assignment
     */
    boolean casEntryFieldLong(int entryIndex, int fieldIndex, long expectedValue, long newValue);

    /**
     * Copy both the key and the value references from another entry array.
     * @param other The entry array to copy from
     * @param srcEntryIndex The source entry index (from the other array)
     * @param destEntryIndex the destination entry index (to this array)
     * @param fieldCount the number of fields to copy
     */
    void copyEntryFrom(EntryArrayInterface other, int srcEntryIndex, int destEntryIndex, int fieldCount);
}
