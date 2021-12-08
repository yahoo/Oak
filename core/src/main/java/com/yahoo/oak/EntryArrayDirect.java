/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class EntryArrayDirect implements EntryArray {

    // We store the buffer object here to avoid the buffer collection by the GC
    // The buffer is initialized to 0 - this is important!
    private final ByteBuffer buffer;

    // The direct address to the entries
    private final long entriesAddress;

    // Number of primitive fields in each entry
    private final int fieldCount;

    // number of entries to be maximally held
    private final int entryCount;

    /**
     * @param entryCount how many entries should this instance keep at maximum
     * @param fieldCount the number of fields (64bit) in each entry
     */
    EntryArrayDirect(int entryCount, int fieldCount) {
        this.fieldCount = fieldCount;
        this.entryCount = entryCount;
        this.buffer = ByteBuffer.allocateDirect(entryCount * fieldCount * Long.BYTES);
        this.entriesAddress = ((DirectBuffer) this.buffer).address();
    }

    /**
     * Converts external entry-index and field-index to the field's direct memory address.
     * <p>
     * @param entryIndex the index of the entry
     * @param fieldIndex the field (long) index inside the entry
     * @return the field's direct memory address
     */
    private long entryMemoryAddress(int entryIndex, int fieldIndex) {
        int offset = (entryIndex * fieldCount) + fieldIndex;
        return entriesAddress + ((long) offset * (long) Long.BYTES);
    }

    /* ########################################################################################
       # EntryArray Interface
       ######################################################################################## */

    /** {@inheritDoc} */
    @Override
    public int entryCount() {
        return entryCount;
    }

    /** {@inheritDoc} */
    @Override
    public int fieldCount() {
        return fieldCount;
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        UnsafeUtils.UNSAFE.setMemory(null, entriesAddress, buffer.capacity(), (byte) 0);
    }

    /** {@inheritDoc} */
    @Override
    public long getEntryFieldLong(int entryIndex, int fieldIndex) {
        return UnsafeUtils.getLong(entryMemoryAddress(entryIndex, fieldIndex));
    }

    /** {@inheritDoc} */
    @Override
    public void setEntryFieldLong(int entryIndex, int fieldIndex, long value) {
        UnsafeUtils.putLong(entryMemoryAddress(entryIndex, fieldIndex), value);
    }

    /** {@inheritDoc} */
    @Override
    public boolean casEntryFieldLong(int entryIndex, int fieldIndex, long expectedValue, long newValue) {
        return UnsafeUtils.UNSAFE.compareAndSwapLong(null,
                entryMemoryAddress(entryIndex, fieldIndex),
                expectedValue, newValue);
    }

    /** {@inheritDoc} */
    @Override
    public void copyEntryFrom(EntryArray other, int srcEntryIndex, int destEntryIndex, int fieldCount) {
        assert fieldCount <= this.fieldCount;
        EntryArrayDirect o = (EntryArrayDirect) other;
        UnsafeUtils.UNSAFE.copyMemory(
                o.entryMemoryAddress(srcEntryIndex, 0),
                this.entryMemoryAddress(destEntryIndex, 0),
                (long) fieldCount * Long.BYTES
        );
    }
}
