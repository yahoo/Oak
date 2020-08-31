/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * ReferenceCodec is responsible to encode and decode in one reference (long) all the information
 * needed to access a memory. ReferenceCodec knows how to mark a reference as deleted and to
 * check for reference being deleted.
 *
 * A reference is composed of 3 parameters: first, second and third.
 * All these parameters are squashed together into one long for easy representation.
 * The derived concrete classes represent codecs for different types of references and
 * can use the parameters for different purposes.
 *
 * IMPORTANT: The 3 parameters (first, second and third) cannot be negative numbers!
 * (As negatives have prefix of ones.)
 */
abstract class ReferenceCodec {
    protected static final int  INVALID_BIT_SIZE = -1;

    private int firstBitSize = 0;
    private int secondBitSize = 0;
    private int thirdBitSize = 0;

    private final int secondShift;
    private final int thirdShift;

    private final long firstMask;
    private final long secondMask;
    private final long thirdMask;

    protected final BlockMemoryAllocator allocator; // common field for all concrete derived references

    /*------- Constructor -------*/

    /**
     * Construct the codec setting the number of bits to be used for each parameter
     * The bit sizes of 2 parameters can be given and the third is always the remaining bits.
     * @param firstBitSizeLimit an upper limit on the size of the first parameter (exclusive)
     *                          if invalid calculate according to other two limits
     * @param secondBitSizeLimit an upper limit on the size of the second parameter (exclusive)
     *                          if invalid calculate according to other two limits
     * @param thirdBitSizeLimit an upper limit on the size of the third parameter (exclusive)
     * @param allocator
     */
    protected ReferenceCodec(int firstBitSizeLimit, int secondBitSizeLimit, int thirdBitSizeLimit,
        BlockMemoryAllocator allocator) {

        if (thirdBitSizeLimit == INVALID_BIT_SIZE) {
            assert !((secondBitSizeLimit == INVALID_BIT_SIZE) && (firstBitSizeLimit == INVALID_BIT_SIZE));
            this.firstBitSize = firstBitSizeLimit;
            this.secondBitSize = secondBitSizeLimit;
            this.thirdBitSize = Long.SIZE - firstBitSize - secondBitSize;
        } else if (secondBitSizeLimit == INVALID_BIT_SIZE) {
            assert !((thirdBitSizeLimit == INVALID_BIT_SIZE) && (firstBitSizeLimit == INVALID_BIT_SIZE));
            this.firstBitSize = firstBitSizeLimit;
            this.thirdBitSize = thirdBitSizeLimit;
            this.secondBitSize = Long.SIZE - firstBitSize - thirdBitSize;
        } else if (firstBitSizeLimit == INVALID_BIT_SIZE) {
            assert !((thirdBitSizeLimit == INVALID_BIT_SIZE) && (secondBitSizeLimit == INVALID_BIT_SIZE));
            this.secondBitSize = secondBitSizeLimit;
            this.thirdBitSize = thirdBitSizeLimit;
            this.firstBitSize = Long.SIZE - secondBitSize - thirdBitSize;
        }

        assert (this.firstBitSize > 0 || this.secondBitSize > 0 || this.thirdBitSize > 0):
            String.format(
                "Not enough bits to encode a reference: firstBitSizeLimit=%,d, secondBitSizeLimit=%,d.",
                firstBitSizeLimit, secondBitSizeLimit);

        this.secondShift = this.firstBitSize;
        this.thirdShift  = this.firstBitSize + this.secondBitSize;

        this.firstMask  = mask(this.firstBitSize);
        this.secondMask = mask(this.secondBitSize);
        this.thirdMask  = mask(this.thirdBitSize);

        this.allocator = allocator;
    }

    /*------- Static helpers -------*/

    /**
     * @param size the value to encode
     * @return the required bits to encode the value (exclusive)
     */
    protected static int requiredBits(long size) {
        return (int) Math.ceil(Math.log(size) / Math.log(2));
    }

    private static long mask(int size) {
        return (1L << size) - 1L;
    }

    @Override
    public String toString() {
        return String.format(
            "ReferenceCodec(first parameter size: %d bits, second parameter size: %d bits," +
                " third parameter size: %d bits)",
                this.firstBitSize, this.secondBitSize, this.thirdBitSize);
    }

    /*------- Internal abstract helpers -------*/
    /* The ability to get the first parameter value from a slice */
    protected abstract long getFirst(Slice s);

    /* The ability to get the second parameter value from a slice */
    protected abstract long getSecond(Slice s);

    /* The ability to get the third parameter value from a slice */
    protected abstract long getThird(Slice s);

    /* The ability to get the first parameter value from a slice */
    protected abstract long getFirstForDelete(long reference);

    /* The ability to get the second parameter value from a slice */
    protected abstract long getSecondForDelete(long reference);

    /* The ability to get the third parameter value from a slice */
    protected abstract long getThirdForDelete(long reference);

    /* The ability to set the slice with all 3 parameters */
    protected abstract void setAll(Slice s, long first, long second, long third);

    /*------- User Interface -------*/

    // check if reference is invalid, according to concreete implementation
    abstract boolean isReferenceValid(long reference);

    // check is reference deleted should be applied according to reference type
    abstract boolean isReferenceDeleted(long reference);

    // invoked (only within assert statement) to check
    // the consistency and correctness of the reference encoding
    abstract boolean isReferenceConsistent(long reference);

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
    long encode(final Slice s) {
        long first  = getFirst(s);
        long second = getSecond(s);
        long third  = getThird(s);

        return  encode(first, second, third);
    }

    /** Present the reference as it needs to be when the target is deleted
     * @param reference to alter
     * @return the encoded reference
     */
    long alterForDelete(final long reference) {
        long first  = getFirstForDelete(reference);
        long second = getSecondForDelete(reference);
        long third  = getThirdForDelete(reference);

        return  encode(first, second, third);
    }

    /**
     * @param s         the object to update
     * @param reference the reference to decode
     * @return true if the allocation reference is valid
     */
    boolean decode(final Slice s, final long reference){
        if (!isReferenceValid(reference)) {
            s.invalidate();
            return false;
        }

        int first  = getFirst(reference);
        int second = getSecond(reference);
        int third  = getThird(reference);

        setAll(s, first, second, third);
        return !isReferenceDeleted(reference);
    }


    private long encode(long first, long second, long third) {
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


    /* To be used by derived classes */
    protected int getFirst(final long reference) {
        return  (int) (reference & firstMask);
    }

    protected int getSecond(final long reference) {
        return  (int) ((reference >>> secondShift) & secondMask);
    }
    protected int getThird(final long reference) {
        return  (int) ((reference >>> thirdShift) & thirdMask);
    }
}
