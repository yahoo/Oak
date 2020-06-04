/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Test;

public class UnsafeUtilsTest {
    static final int[] TEST_NUMBERS = new int[]{0, 1, 2, Integer.MAX_VALUE, -1, -2, Integer.MIN_VALUE};

    int[] longToInts(long l) {
        return new int[]{
                (int) (l & UnsafeUtils.LONG_INT_MASK),
                (int) ((l >>> Integer.SIZE) & UnsafeUtils.LONG_INT_MASK)
        };
    }

    public void singleIntLongTest(int i1, int i2) {
        long combine = UnsafeUtils.intsToLong(i1, i2);
        int[] res = longToInts(combine);
        Assert.assertEquals(i1, res[0]);
        Assert.assertEquals(i2, res[1]);
    }

    @Test
    public void testIntsToLong() {
        for (int i1 : TEST_NUMBERS) {
            for (int i2 : TEST_NUMBERS) {
                singleIntLongTest(i1, i2);
            }
        }
    }
}
