/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * A Sequential-Expanding reference doesn't impose any memory manager related actions upon any access.
 * A Sequential-Expanding reference is composed of 3 parameters:
 *
 * block ID (first), offset (second) and length (third)
 *
 * All these parameters may be squashed together into one long for easy representation.
 * Using different number of bits for each parameter may incur different limitations on their sizes.
 *
 * Number of bits used for BlockID + offset gives the size of the memory that can be referenced
 * with ReferenceCodecSeqExpand.
 *
 * If block size is 256MB = 2^28 --> takes 28 bits
 * Then BlockID+offset have 36 bits for their representation.
 * Total memory 64GB
 *
 *
 * The Sequential-Expanding reference codec encodes the reference of the Sequential-Expanding slices
 * into a single long primitive (64 bit).
 * For example, for the block size 256MB, we need 28 bits to encode the offset
 * and additional 28 bits to encode the length.
 * So, the remaining 8 bits can encode the block id, which will limit the maximal number of blocks to 256.
 * Thus, the key/value reference encoding when using this block size (256MB) will be as follows:
 *
 *    LSB                                       MSB
 *     |     offset     |     length     | block |
 *     |     28 bit     |     28 bit     | 8 bit |
 *      0             27 28            55 56   63
 *
 * From that, we can derive that the maximal number of 1K items that can be allocated is ~128 million (2^26).
 * Note: these limitations will change for different block sizes. */

class ReferenceCodecSeqExpand extends ReferenceCodec {
    static final long INVALID_REFERENCE = 0;
    /**
     * Initialize the codec with size block-size and value length limits.
     * These limits will inflict a limit on the maximal number of blocks (the remaining bits).
     * offset and length can only be as long as a size of the block.
     * @param offsetSizeLimit an upper limit on the size of a block (exclusive)
     * @param lengthSizeLimit    an upper limit on the data length (exclusive)
     * @param allocator
     */
    ReferenceCodecSeqExpand(long offsetSizeLimit, long lengthSizeLimit, BlockMemoryAllocator allocator) {
        super(INVALID_BIT_SIZE, // bits# to represent block id are calculated upon other parameters
            requiredBits(offsetSizeLimit),   // bits# to represent offset
            requiredBits(lengthSizeLimit)  // bits# to represent length
        );
    }

    @Override
    protected long getFirst(Slice s) {
        return (long) s.getAllocatedBlockID();
    }

    @Override
    protected long getSecond(Slice s) {
        return (long) s.getAllocatedOffset();
    }

    @Override
    protected long getThird(Slice s) {
        return (long) s.getAllocatedLength();
    }

    @Override
    protected long getFirstForDelete(long reference) {
        return getFirst(reference);
    }

    @Override
    protected long getSecondForDelete(long reference) {
        return getSecond(reference);
    }

    @Override
    protected long getThirdForDelete(long reference) {
        return getThird(reference);
    }

    @Override
    protected void setAll(Slice s, long blockID, long offset, long length, long reference) {
        s.associateReferenceDecoding((int) blockID, (int) offset, (int) length, reference);
    }

    @Override
    boolean isReferenceDeleted(long reference) {
        return false;
    }

    @Override
    boolean isReferenceConsistent(long reference) {
        return true;
    }

    @Override
    boolean isReferenceValid(long reference) {
        return reference != INVALID_REFERENCE;
    }
}
