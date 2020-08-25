/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * A reference that is involved in memory management is composed of 3 parameters:
 * block ID, offset and version.
 *
 * 0...            ...42 | 43... ...63
 * block ID   |  offset  | version
 * first         second      third
 *
 * All these parameters may be squashed together into one long for easy representation.
 * Using different number of bits for each parameter may incur different limitations on their sizes.
 */
class ReferenceCodecMM extends ReferenceCodec{

    // All Oak instances on one machine are expected to reference to no more than 4TB of RAM.
    // 4TB = 2^42 bytes
    // blockIDBitSize + offsetBitSize = BITS_FOR_MAXIMUM_RAM
    // The number of bits required to represent such memory:
    private static final int    BITS_FOR_MAXIMUM_RAM = 42;
    private static final long   VERSION_DELETE_BIT_MASK = (1 << (Long.SIZE - BITS_FOR_MAXIMUM_RAM -1));
    public  static final int    INVALID_VERSION = 0;
    private static final long   INVALID_MM_REFERENCE = 0;

    /**
     * Initialize the codec with offset in the size of block.
     * This will inflict a limit on the maximal number of blocks - size of block ID.
     * The remaining bits out of maximum RAM (BITS_FOR_MAXIMUM_RAM)
     * @param blockSize an upper limit on the size of a block (exclusive)
     * @param allocator
     *
     */
    ReferenceCodecMM(long blockSize, BlockMemoryAllocator allocator) {
        super(BITS_FOR_MAXIMUM_RAM - ReferenceCodecDirect.requiredBits(blockSize),
            ReferenceCodecDirect.requiredBits(blockSize), INVALID_BIT_SIZE, allocator);
        // and the rest goes for version (currently 22 bits)
    }


    @Override protected long getFirst(Slice s) {
        return (long) s.getAllocatedBlockID();
    }

    @Override protected long getSecond(Slice s) {
        return (long) s.getAllocatedOffset();
    }

    @Override protected long getThird(Slice s) {
        int ver = s.getVersion();
        return (long) ver;
    }

    @Override protected long getFirstForDelete(long reference) {
        return INVALID_MM_REFERENCE;
    }

    @Override protected long getSecondForDelete(long reference) {
        return INVALID_MM_REFERENCE;
    }

    @Override protected long getThirdForDelete(long reference) {
        long v = getThird(reference);
        // The set the MSB (the left-most bit out of 22 is delete bit)
        v |= VERSION_DELETE_BIT_MASK;
        return (INVALID_MM_REFERENCE | v);
    }

    @Override protected void setAll(Slice s, long first, long second, long third) {
        s.update( ((int) first), ((int) second), Slice.UNDEFINED_LENGTH_OR_OFFSET, ((int) third));
        if (!isVersionDeleted((int) third)) {
            allocator.readByteBuffer(s, (int) first);
        }
    }

    @Override boolean isReferenceValid(long reference) {
        return reference != INVALID_MM_REFERENCE;
    }

    @Override boolean isReferenceDeleted(final Slice s) {
        int v = s.getVersion();
        // invalid version means slice is not allocated and thus not deleted
        if (v == INVALID_VERSION) {
            return false;
        }
        v &= VERSION_DELETE_BIT_MASK;
        return (v!=0);
    }

    // invoked only within assert
    boolean isConsistent(final Slice s) {
        if (isReferenceDeleted(s)) {
            return true;
        }
        if (encode(s) == INVALID_MM_REFERENCE) {
            return true;
        }
        int v = s.getVersion();
        return (v!=INVALID_VERSION);
    }

    // invoked only within assert
    boolean isReferenceConsistent(long reference) {
        if (reference == INVALID_MM_REFERENCE) {
            return true;
        }
        if (isReferenceDeleted(reference)) {
            return true;
        }
        int v = getThird(reference);
        return (v!=INVALID_VERSION);
    }

    @Override boolean isReferenceDeleted(long reference) {
        return isVersionDeleted((int) getThird(reference));
    }

    private boolean isVersionDeleted(int v) {
        int vv = (int) ((long) v & VERSION_DELETE_BIT_MASK);
        return (vv!=INVALID_VERSION);
    }

    static long getInvalidReference() {
        return INVALID_MM_REFERENCE;
    }
}
