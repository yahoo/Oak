package com.oath.oak;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class UnsafeUtilsTest {
    static final int[] testNumbers = new int[] {0, 1, 2, Integer.MAX_VALUE, -1, -2, Integer.MIN_VALUE};

    int[] longToInts(long l) {
        return new int [] {
                (int) (l & UnsafeUtils.LONG_INT_MASK),
                (int) ((l >>> Integer.SIZE) & UnsafeUtils.LONG_INT_MASK)
        };
    }

    public void singleIntLongTest(int i1, int i2) {
        long combine = UnsafeUtils.intsToLong(i1, i2);
        int[] res = longToInts(combine);
        assertEquals(i1, res[0]);
        assertEquals(i2, res[1]);
    }

    @Test
    public void testIntsToLong() {
        for (int i1 : testNumbers) {
            for (int i2 : testNumbers) {
                singleIntLongTest(i1, i2);
            }
        }
    }
}
