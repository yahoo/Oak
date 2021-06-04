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

    @Test
    public void testCopyToArray() {
        final int sz = 10;
        final long allocSize = sz * Integer.BYTES;
        final int[] expected = new int[sz];

        long address = UnsafeUtils.allocateMemory(allocSize);

        try {
            for (int i = 0; i < sz; i++) {
                expected[i] = i;
                UnsafeUtils.putInt(address + i * Integer.BYTES, i);
            }

            final int[] result = new int[sz];
            long copySize = UnsafeUtils.copyToArray(address, result, sz);
            Assert.assertEquals(allocSize, copySize);
            Assert.assertArrayEquals(expected, result);
        } finally {
            UnsafeUtils.freeMemory(address);
        }
    }

    @Test
    public void testCopyFromArray() {
        final int sz = 10;
        final long allocSize = sz * Integer.BYTES;
        final int[] expected = new int[sz];

        for (int i = 0; i < sz; i++) {
            expected[i] = i;
        }

        long address = UnsafeUtils.allocateMemory(sz * Integer.BYTES);
        try {
            long copySize = UnsafeUtils.copyFromArray(expected, address, sz);
            Assert.assertEquals(allocSize, copySize);

            for (int i = 0; i < sz; i++) {
                Assert.assertEquals(expected[i], UnsafeUtils.getInt(address + i * Integer.BYTES));
            }
        } finally {
            UnsafeUtils.freeMemory(address);
        }
    }
}
