/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/* Encapsulates a context, from when an key/value/entry operation has began until the it was completed */
class ThreadContext {

    /*-----------------------------------------------------------
     * Entry Context
     *-----------------------------------------------------------*/

    /* The index of the key's entry in EntryArray */
    int entryIndex;

    /* key is used for easier access to the off-heap memory */
    final KeyBuffer key;

    /* The state of the value */
    EntryArray.EntryState entryState;

    /* value is used for easier access to the off-heap memory */
    final ValueBuffer value;

    /*-----------------------------------------------------------
     * Value Insertion Context
     *-----------------------------------------------------------*/

    /**
     * This parameter encapsulates the allocation information, from when value write started
     * and until value write was committed. It should not be used for other purposes, just transferred
     * between allocateValue (return parameter) to writeValueCommit (input parameter)
     */
    final ValueBuffer newValue;

    /**
     * Flags if the new allocated value was originated from a move operation.
     * If false, then it is a new allocation and value.isValid() should be false.
     */
    boolean isNewValueForMove;

    /* The key hash and update counter of the found (serialized) entry's key.
     * Relevant and used only for OakHash */
    long keyHashAndUpdateCnt;

    /* The key hash of the key being inserted/deleted/looked-for by this thread. Relevant and used only for OakHash */
    int operationKeyHash;

    /*-----------------------------------------------------------
     * Result Context
     *-----------------------------------------------------------*/

    final Result result;

    /*-----------------------------------------------------------
     * Temporary Context
     *-----------------------------------------------------------*/

    final KeyBuffer tempKey;
    final ValueBuffer tempValue;

    ThreadContext(MemoryManager kmm, MemoryManager vmm) {
        entryIndex = EntryArray.INVALID_ENTRY_INDEX;
        entryState = EntryArray.EntryState.UNKNOWN;
        isNewValueForMove = false;

        this.key = new KeyBuffer(kmm.getEmptySlice());
        this.value = new ValueBuffer(vmm.getEmptySlice());
        this.newValue = new ValueBuffer(vmm.getEmptySlice());
        this.result = new Result();
        this.tempKey = new KeyBuffer(kmm.getEmptySlice());
        this.tempValue = new ValueBuffer(vmm.getEmptySlice());

        this.keyHashAndUpdateCnt = EntryHashSet.INVALID_KEY_HASH_AND_UPD_CNT;
    }

    void invalidate() {
        entryIndex = EntryArray.INVALID_ENTRY_INDEX;
        key.invalidate();
        value.invalidate();
        newValue.invalidate();
        result.invalidate();
        entryState = EntryArray.EntryState.UNKNOWN;
        isNewValueForMove = false;
        this.keyHashAndUpdateCnt = EntryHashSet.INVALID_KEY_HASH_AND_UPD_CNT;
        // No need to invalidate the temporary buffers
    }

    /**
     * Initialize the entry context index to be used by methods that manages the key/value of this context.
     * The entry index is stored in the context so it can be used later by other methods without passing the
     * entry index explicitly as a parameter.
     *
     * @param entryIndex the entry index to update
     */
    void initEntryContext(int entryIndex) {
        this.entryIndex = entryIndex;
    }

    /**
     * We consider a key to be valid if the entry referred to a valid allocation.
     *
     * @return does the entry have a valid key
     */
    boolean isKeyValid() {
        return key.isAssociated();
    }

    /**
     * See {@code EntryState.isValid()} for more details.
     *
     * @return does the entry have a valid value
     */
    boolean isValueValid() {
        return entryState.isValid();
    }
}
