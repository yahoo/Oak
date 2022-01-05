/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.util.Arrays;

/**
 * Stores the entry array as Java long array (on-heap).
 */
public class EntryArrayHeap implements EntryArrayInternal {

    // The array is initialized to 0 - this is important!
    private final long[] entries;

    // Number of primitive fields in each entry
    private final int fieldCount;

    // number of entries to be maximally held
    private final int entryCount;

    /**
     * @param entryCount how many entries should this instance keep at maximum
     * @param fieldCount the number of fields (64bit) in each entry
     */
    EntryArrayHeap(int entryCount, int fieldCount) {
        this.fieldCount = fieldCount;
        this.entryCount = entryCount;
        this.entries = new long[entryCount * fieldCount];
    }

    /**
     * Converts external entry-index and field-index to the field's direct memory address.
     * <p>
     * @param entryIndex the index of the entry
     * @param fieldIndex the field (long) index inside the entry
     * @return the field's offset in the array
     */
    private int entryOffset(int entryIndex, int fieldIndex) {
        return entryIndex * fieldCount + fieldIndex;
    }

    /** {@inheritDoc} */
    @Override
    public int entryCount() {
        return entryCount;
    }

    /** {@inheritDoc} */
    @Override
    public int fieldCount() {
        return entryCount;
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        Arrays.fill(entries, 0);
    }

    /** {@inheritDoc} */
    @Override
    public long getEntryFieldLong(int entryIndex, int fieldIndex) {
        return entries[entryOffset(entryIndex, fieldIndex)];
    }

    /** {@inheritDoc} */
    @Override
    public void setEntryFieldLong(int entryIndex, int fieldIndex, long value) {
        entries[entryOffset(entryIndex, fieldIndex)] = value;
    }

    /** {@inheritDoc} */
    @Override
    public boolean casEntryFieldLong(int entryIndex, int fieldIndex, long expectedValue, long newValue) {
        int offset = Unsafe.ARRAY_LONG_BASE_OFFSET +
                entryOffset(entryIndex, fieldIndex) * Unsafe.ARRAY_LONG_INDEX_SCALE;
        return UnsafeUtils.UNSAFE.compareAndSwapLong(entries, offset, expectedValue, newValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyEntryFrom(EntryArrayInternal other, int srcEntryIndex, int destEntryIndex, int fieldCount) {
        assert fieldCount <= this.fieldCount;
        EntryArrayHeap o = (EntryArrayHeap) other;
        System.arraycopy(o.entries,  // source entries array
                o.entryOffset(srcEntryIndex, 0),
                entries,                        // this entries array
                o.entryOffset(destEntryIndex, 0),
                fieldCount);
    }
}
