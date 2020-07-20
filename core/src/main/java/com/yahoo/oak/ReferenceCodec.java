/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * A reference is composed of 3 parameters: block ID, offset and length.
 * All these parameters may be squashed together into one long for easy representation.
 * Using different number of bits for each parameter may incur different limitations on their sizes.
 */
class ReferenceCodec {
    public static final long INVALID_REFERENCE = 0;

    final int offsetBitSize;
    final int lengthBitSize;
    final int blockBitSize;

    final int lengthShift;
    final int blockShift;

    final long offsetMask;
    final long lengthMask;
    final long blockMask;

    /**
     * Initialize the codec with size block-size and value length limits.
     * These limits will inflict a limit on the maximal number of blocks (the remaining bits).
     *
     * @param blockSizeLimit an upper limit on the size of a block (exclusive)
     * @param lengthLimit    an upper limit on the data length (exclusive)
     */
    ReferenceCodec(long blockSizeLimit, long lengthLimit) {
        this.offsetBitSize = requiredBits(blockSizeLimit);
        this.lengthBitSize = requiredBits(lengthLimit);
        this.blockBitSize = Long.SIZE - offsetBitSize - lengthBitSize;
        assert this.blockBitSize > 0 : String.format(
                "Not enough bits to encode a reference: blockSizeLimit=%,d, lengthLimit=%,d.",
                blockSizeLimit, lengthLimit);

        this.lengthShift = this.offsetBitSize;
        this.blockShift = this.lengthShift + this.lengthBitSize;

        this.offsetMask = mask(this.offsetBitSize);
        this.lengthMask = mask(this.lengthBitSize);
        this.blockMask = mask(this.blockBitSize);
    }

    /**
     * @param size the value to encode
     * @return the required bits to encode the value (exclusive)
     */
    static int requiredBits(long size) {
        return (int) Math.ceil(Math.log(size) / Math.log(2));
    }

    static long mask(int size) {
        return (1L << size) - 1L;
    }

    static boolean isValidReference(long reference) {
        return reference != INVALID_REFERENCE;
    }

    @Override
    public String toString() {
        return String.format("ReferenceCodec(offset=%d bit, length=%d bit, block=%d bit)",
                this.offsetBitSize, this.lengthBitSize, this.blockBitSize);
    }

    /*
    In the implementation of encode/decode methods, we make two assumptions that
    are true in all modern architectures:
      (1) CPU level parallelism: independent instructions will be executed simultaneously.
      (2) Shift complexity: shift (>>) op takes a fixed number of cycles
                            (its complexity is independent of the shift size).
     This means that the three mask+shift operations will be executed (and finished) simultaneously.
     */

    /**
     * @param s the object to encode
     * @return the encoded reference
     */
    public long encode(final Slice s) {
        // These checks validates that the chosen encoding is sufficient for the current use-case.
        if ((((long) s.getAllocatedOffset()) & ~offsetMask) != 0 ||
                (((long) s.getAllocatedLength()) & ~lengthMask) != 0 ||
                (((long) s.getAllocatedBlockID()) & ~blockMask) != 0) {
            throw new IllegalArgumentException(String.format("%s has insufficient capacity to encode %s", this, s));
        }

        long offsetPart = ((long) s.getAllocatedOffset()) & offsetMask;
        long lengthPart = (((long) s.getAllocatedLength()) & lengthMask) << lengthShift;
        long blockPart = (((long) s.getAllocatedBlockID()) & blockMask) << blockShift;
        return offsetPart | lengthPart | blockPart;
    }

    /**
     * @param s         the object to update
     * @param reference the reference to decode
     * @return true if the allocation reference is valid
     */
    public boolean decode(final Slice s, final long reference) {
        if (!isValidReference(reference)) {
            s.invalidate();
            return false;
        }

        int offset = (int) (reference & offsetMask);
        int length = (int) ((reference >>> lengthShift) & lengthMask);
        int blockId = (int) ((reference >>> blockShift) & blockMask);
        s.update(blockId, offset, length);
        return true;
    }
}
