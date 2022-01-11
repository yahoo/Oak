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
class ReferenceCodec extends UnionCodec {
    static final long INVALID_REFERENCE = 0;

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
    protected ReferenceCodec(int firstBitSizeLimit, int secondBitSizeLimit, int thirdBitSizeLimit) {
        super(firstBitSizeLimit, secondBitSizeLimit, thirdBitSizeLimit, Long.SIZE);
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
    protected ReferenceCodec(int firstBitSizeLimit, int secondBitSizeLimit) {
        super(firstBitSizeLimit, secondBitSizeLimit, Long.SIZE);
    }

    /*------- Internal helpers -------*/

    /* The ability to get the first parameter value for deleted reference */
    protected long getFirstForDelete(long reference) {
        return getFirst(reference);
    }

    /* The ability to get the second parameter value for deleted referenc */
    protected long getSecondForDelete(long reference) {
        return getSecond(reference);
    }

    /* The ability to get the third parameter value for deleted referenc */
    protected long getThirdForDelete(long reference) {
        return getThird(reference);
    }

    /*------- User Interface -------*/

    // check if reference is invalid, according to concreete implementation
    boolean isReferenceValid(long reference) {
        return reference != INVALID_REFERENCE;
    }

    // check is reference deleted should be applied according to reference type
    boolean isReferenceDeleted(long reference) {
        return false;
    }

    // invoked (only within assert statement) to check
    // the consistency and correctness of the reference encoding
    boolean isReferenceConsistent(long reference) {
        return true;
    }

    /*
    In the implementation of encode/decode methods, we make two assumptions that
    are true in all modern architectures:
      (1) CPU level parallelism: independent instructions will be executed simultaneously.
      (2) Shift complexity: shift (>>) op takes a fixed number of cycles
                            (its complexity is independent of the shift size).
     This means that the three mask+shift operations will be executed (and finished) simultaneously.
     */


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
}
