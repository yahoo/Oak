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
    static final int AUTO_CALCULATE_BIT_SIZE = -1;
    private static final int INVALID_THIRD_VALUE = 0;

    private int firstBitSize = 0;
    private int secondBitSize = 0;
    private int thirdBitSize = 0;

    private final int secondShift;
    private final int thirdShift;

    private final long firstMask;
    private final long secondMask;
    private final long thirdMask;

    private final int unionSizeInBits;

    /*------- Constructor -------*/

    /**
     * Construct the codec setting the number of bits to be used for each parameter
     * The bit sizes of 2 parameters can be given and the third is always the remaining bits.
     * @param firstBitSizeLimit an upper limit on the size of the first parameter (exclusive)
     *                          if invalid calculate according to other two limits
     * @param secondBitSizeLimit an upper limit on the size of the second parameter (exclusive)
     *                          if invalid calculate according to other two limits
     * @param thirdBitSizeLimit an upper limit on the size of the third parameter (exclusive)
     * @param unionSizeInBits   the size of the entire union in bits, either Long or Integer
     */
    protected UnionCodec(int firstBitSizeLimit, int secondBitSizeLimit, int thirdBitSizeLimit,
        int unionSizeInBits) {

        if (thirdBitSizeLimit == AUTO_CALCULATE_BIT_SIZE) {
            assert (secondBitSizeLimit != AUTO_CALCULATE_BIT_SIZE) && (firstBitSizeLimit != AUTO_CALCULATE_BIT_SIZE);
            this.firstBitSize = firstBitSizeLimit;
            this.secondBitSize = secondBitSizeLimit;
            this.thirdBitSize = unionSizeInBits - firstBitSize - secondBitSize;
        } else if (secondBitSizeLimit == AUTO_CALCULATE_BIT_SIZE) {
            // thirdBitSizeLimit is valid
            assert (firstBitSizeLimit != AUTO_CALCULATE_BIT_SIZE);
            this.firstBitSize = firstBitSizeLimit;
            this.thirdBitSize = thirdBitSizeLimit;
            this.secondBitSize = unionSizeInBits - firstBitSize - thirdBitSize;
        } else if (firstBitSizeLimit == AUTO_CALCULATE_BIT_SIZE) {
            this.secondBitSize = secondBitSizeLimit;
            this.thirdBitSize = thirdBitSizeLimit;
            this.firstBitSize = unionSizeInBits - secondBitSize - thirdBitSize;
        }

        this.unionSizeInBits = unionSizeInBits;

        assert (this.firstBitSize >= 0 || this.secondBitSize >= 0 || this.thirdBitSize >= 0) :
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
     * @param unionSizeInBits
     */
    protected UnionCodec(int firstBitSizeLimit, int secondBitSizeLimit, int unionSizeInBits) {

        if (secondBitSizeLimit == AUTO_CALCULATE_BIT_SIZE) {
            assert (firstBitSizeLimit != AUTO_CALCULATE_BIT_SIZE);
            this.firstBitSize = firstBitSizeLimit;
            this.secondBitSize = unionSizeInBits - firstBitSize;
        } else if (firstBitSizeLimit == AUTO_CALCULATE_BIT_SIZE) {
            this.secondBitSize = secondBitSizeLimit;
            this.firstBitSize = unionSizeInBits - secondBitSize;
        }

        this.unionSizeInBits = unionSizeInBits;
        assert (this.firstBitSize > 0 || this.secondBitSize > 0) :
            String.format(
                "Not enough bits to encode a union: firstBitSizeLimit=%,d, secondBitSizeLimit=%,d.",
                firstBitSizeLimit, secondBitSizeLimit);

        this.secondShift = this.firstBitSize;
        this.firstMask  = mask(this.firstBitSize);
        this.secondMask = mask(this.secondBitSize);

        // invalidate third part parameters
        this.thirdShift = INVALID_THIRD_VALUE;
        this.thirdMask  = INVALID_THIRD_VALUE;
    }

    /*--------- Static helpers ---------*/

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

    int getFirst(final long union) {
        return  (int) (union & firstMask);
    }

    int getFirst(final int union) {
        return  (int) (union & firstMask);
    }

    int getSecond(final long union) {
        return  (int) ((union >>> secondShift) & secondMask);
    }

    int getSecond(final int union) {
        return  (int) ((union >>> secondShift) & secondMask);
    }

    int getThird(final long union) {
        return  (int) ((union >>> thirdShift) & thirdMask);
    }

    int getThird(final int union) {
        return  (int) ((union >>> thirdShift) & thirdMask);
    }

    int getFirstBitSize() {
        return firstBitSize;
    }

    int getSecondBitSize() {
        return secondBitSize;
    }

    int getThirdBitSize() {
        return thirdBitSize;
    }

}
