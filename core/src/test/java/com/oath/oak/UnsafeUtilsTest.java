package com.oath.oak;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class UnsafeUtilsTest {

    @Test
    public void testIntsToLong(){
        int i1 = 1, i2 = 2;
        long combine = UnsafeUtils.intsToLong(i1, i2);
        int[] res = UnsafeUtils.longToInts(combine);
        assertEquals(i1, res[0]);
        assertEquals(i2, res[1]);
        i2 = -2;
        combine = UnsafeUtils.intsToLong(i1, i2);
        res = UnsafeUtils.longToInts(combine);
        assertEquals(i1, res[0]);
        assertEquals(i2, res[1]);
        i1 = -1;
        combine = UnsafeUtils.intsToLong(i1, i2);
        res = UnsafeUtils.longToInts(combine);
        assertEquals(i1, res[0]);
        assertEquals(i2, res[1]);
        i2 = 2;
        combine = UnsafeUtils.intsToLong(i1, i2);
        res = UnsafeUtils.longToInts(combine);
        assertEquals(i1, res[0]);
        assertEquals(i2, res[1]);
    }

}
