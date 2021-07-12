/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * UnionCodec is combining two or three integers into one long
 *
 * IMPORTANT: The 3 parameters (first, second and third) cannot be negative numbers!
 * (As negatives have prefix of ones.)
 */
class UnionCodec {
    static final int INVALID_BIT_SIZE = -1;

    private int firstBitSize = 0;
    private int secondBitSize = 0;
    private int thirdBitSize = 0;

    private final int secondShift;
    private final int thirdShift;

    private final long firstMask;
    private final long secondMask;
    private final long thirdMask;

    /*------- Constructor -------*/

    /**
     * Construct the codec setting the number of bits to be used for each parameter
     * The bit sizes of 2 parameters can be given and the third is always the remaining bits.
     * @param firstBitSizeLimit an upper limit on the size of the first parameter (exclusive)
     *                          if invalid calculate according to other two limits
     * @param secondBitSizeLimit an upper limit on the size of the second parameter (exclusive)
     *                          if invalid calculate according to other two limits
     * @param thirdBitSizeLimit an upper limit on the size of the third parameter (exclusive)
     */
    protected UnionCodec(int firstBitSizeLimit, int secondBitSizeLimit, int thirdBitSizeLimit) {

        if (thirdBitSizeLimit == INVALID_BIT_SIZE) {
            assert (secondBitSizeLimit != INVALID_BIT_SIZE) && (firstBitSizeLimit != INVALID_BIT_SIZE);
            this.firstBitSize = firstBitSizeLimit;
            this.secondBitSize = secondBitSizeLimit;
            this.thirdBitSize = Long.SIZE - firstBitSize - secondBitSize;
        } else if (secondBitSizeLimit == INVALID_BIT_SIZE) {
            // thirdBitSizeLimit is valid
            assert (firstBitSizeLimit != INVALID_BIT_SIZE);
            this.firstBitSize = firstBitSizeLimit;
            this.thirdBitSize = thirdBitSizeLimit;
            this.secondBitSize = Long.SIZE - firstBitSize - thirdBitSize;
        } else if (firstBitSizeLimit == INVALID_BIT_SIZE) {
            this.secondBitSize = secondBitSizeLimit;
            this.thirdBitSize = thirdBitSizeLimit;
            this.firstBitSize = Long.SIZE - secondBitSize - thirdBitSize;
        }

        assert (this.firstBitSize > 0 || this.secondBitSize > 0 || this.thirdBitSize > 0) :
            String.format(
                "Not enough bits to encode: firstBitSizeLimit=%,d, secondBitSizeLimit=%,d.",
                firstBitSizeLimit, secondBitSizeLimit);

        this.secondShift = this.firstBitSize;
        this.thirdShift  = this.firstBitSize + this.secondBitSize;

        this.firstMask  = mask(this.firstBitSize);
        this.secondMask = mask(this.secondBitSize);
        this.thirdMask  = mask(this.thirdBitSize);
    }

    /**
     * Construct the codec setting the number of bits to be used for each parameter.
     * This instance would combine only 2 integers inside long.
     * The bit sizes of 1 parameters can be given and the third is always the remaining bits.
     * @param firstBitSizeLimit an upper limit on the size of the first parameter (exclusive)
     *                          if invalid calculate according to other two limits
     * @param secondBitSizeLimit an upper limit on the size of the second parameter (exclusive)
     *                          if invalid calculate according to other two limits
     */
    protected UnionCodec(int firstBitSizeLimit, int secondBitSizeLimit) {

        if (secondBitSizeLimit == INVALID_BIT_SIZE) {
            assert (firstBitSizeLimit != INVALID_BIT_SIZE);
            this.firstBitSize = firstBitSizeLimit;
            this.secondBitSize = Long.SIZE - firstBitSize;
        } else if (firstBitSizeLimit == INVALID_BIT_SIZE) {
            this.secondBitSize = secondBitSizeLimit;
            this.firstBitSize = Long.SIZE - secondBitSize;
        }

        assert (this.firstBitSize > 0 || this.secondBitSize > 0) :
            String.format(
                "Not enough bits to encode a reference: firstBitSizeLimit=%,d, secondBitSizeLimit=%,d.",
                firstBitSizeLimit, secondBitSizeLimit);

        this.secondShift = this.firstBitSize;
        this.firstMask  = mask(this.firstBitSize);
        this.secondMask = mask(this.secondBitSize);

        // invalidate third part parameters
        this.thirdShift = INVALID_BIT_SIZE;
        this.thirdMask  = INVALID_BIT_SIZE;
    }

    /*--------- Static helpers ---------*/

    /**
     * @param size the value to encode
     * @return the required bits to encode the value (exclusive)
     */
    static int requiredBits(long size) {
        return (int) Math.ceil(Math.log(size) / Math.log(2));
    }

    protected static long mask(int size) {
        return (1L << size) - 1L;
    }

    @Override
    public String toString() {
        return String.format(
            "UnionCodec(first parameter size: %d bits, second parameter size: %d bits," +
                " third parameter size: %d bits)",
                this.firstBitSize, this.secondBitSize, this.thirdBitSize);
    }

    /*
    In the implementation of encode/decode methods, we make two assumptions that
    are true in all modern architectures:
      (1) CPU level parallelism: independent instructions will be executed simultaneously.
      (2) Shift complexity: shift (>>) op takes a fixed number of cycles
                            (its complexity is independent of the shift size).
     This means that the three mask+shift operations will be executed (and finished) simultaneously.
     */

    long encode(long first, long second) {
        // These checks validates that the chosen encoding is sufficient for the current use-case.
        if ((first & ~firstMask) != 0 || (second & ~secondMask) != 0 ) {
            throw new IllegalArgumentException(
                String.format(
                    "%s has insufficient capacity to encode 2 integers: first %s, second %s",
                    this, first, second));
        }

        long firstPart  = first & firstMask;
        long secondPart = (second & secondMask) << secondShift;

        return firstPart | secondPart ;
    }

    long encode(long first, long second, long third) {
        // These checks validates that the chosen encoding is sufficient for the current use-case.
        if ((first & ~firstMask) != 0 || (second & ~secondMask) != 0 || (third & ~thirdMask) != 0 ) {
            throw new IllegalArgumentException(
                String.format(
                    "%s has insufficient capacity to encode first %s, second %s, and third %s",
                    this, first, second, third));
        }

        long firstPart  = first & firstMask;
        long secondPart = (second & secondMask) << secondShift;
        long thirdPart  = (third & thirdMask)   << thirdShift;

        return firstPart | secondPart | thirdPart;
    }

    int getFirst(final long reference) {
        return  (int) (reference & firstMask);
    }

    int getSecond(final long reference) {
        return  (int) ((reference >>> secondShift) & secondMask);
    }

    int getThird(final long reference) {
        return  (int) ((reference >>> thirdShift) & thirdMask);
    }
}
