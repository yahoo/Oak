/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * A reference that is involved in synchronized and recycling memory management is composed of
 * 3 parameters:block ID, offset and version.
 *
 * 0...            ...21 | 22... ...63
 *          version      | length
 *          first           second
 *
 * All these parameters may be squashed together into one long for easy representation.
 * Using different number of bits for each parameter may incur different limitations on their sizes.
 */
class ReferenceCodecNovaHeader extends ReferenceCodec {

    static final int    INVALID_VERSION = 0;
    static final int    DELETED_REFERENCE = 1;
    static final int    VERSION_SIZE = 22;
    private long versionDeleteBitMASK    = 1; //first bit is the deleted bit
    private long referenceDeleteBitMASK  = 1;


    /**
     * Initialize the codec with offset in the size of block.
     * This will inflict a limit on the maximal number of blocks - size of block ID.
     * The remaining bits out of maximum RAM (BITS_FOR_MAXIMUM_RAM)
     * @param blockSize an upper limit on the size of a block (exclusive)
     * @param allocator
     *
     */
    ReferenceCodecNovaHeader() {
        super(VERSION_SIZE, Long.SIZE - VERSION_SIZE, AUTO_CALCULATE_BIT_SIZE);
        // and the rest goes for version (currently 22 bits)
    }

    @Override
    protected long getThirdForDelete(long reference) {
        long v = getThird(reference);
        // The set the MSB (the left-most bit out of 22 is delete bit)
        v |= versionDeleteBitMASK;
        return (INVALID_REFERENCE | v);
    }

    // invoked only within assert
    boolean isReferenceConsistent(long reference) {
        if (reference == INVALID_REFERENCE) {
            return true;
        }
        if (isReferenceDeleted(reference)) {
            return true;
        }
        int v = getThird(reference);
        return (v != INVALID_VERSION);
    }

    @Override
    boolean isReferenceDeleted(long reference) {
        return ((reference & referenceDeleteBitMASK) == DELETED_REFERENCE);
    }

    boolean isReferenceValidAndNotDeleted(long reference) {
        return (reference != INVALID_REFERENCE &&
            (reference & referenceDeleteBitMASK) == INVALID_REFERENCE);
    }
}
