/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.MurmurHash3;
import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MurmurTest {

    private OakMap<byte[], byte[]> oak;

    @Before
    public void init() {
        oak = OakCommonBuildersFactory.getDefaultByteArrayBuilder().buildOrderedMap();
    }

    @After
    public void finish() {
        oak.close();
    }

    @Test
    public void test32() {
        final byte[] test = new byte[] {1, 2, 3};
        final int expected = MurmurHash3.murmurhash32(test, 0, test.length, 0);
        oak.put(test, test);

        OakBuffer oakBuffer = oak.zc().get(test);
        Assert.assertNotNull(oakBuffer);

        Assert.assertEquals(test.length, oakBuffer.capacity() - Integer.BYTES);

        final int actual = MurmurHash3.murmurhash32(oakBuffer, Integer.BYTES, test.length, 0);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void test128() {
        final byte[] test = new byte[] {1, 2, 3};
        final MurmurHash3.HashCode128 expected = new MurmurHash3.HashCode128();
        MurmurHash3.murmurhash128(test, 0, test.length, 0, expected);
        oak.put(test, test);

        OakBuffer oakBuffer = oak.zc().get(test);
        Assert.assertNotNull(oakBuffer);

        Assert.assertEquals(test.length, oakBuffer.capacity() - Integer.BYTES);

        final MurmurHash3.HashCode128 actual = new MurmurHash3.HashCode128();
        MurmurHash3.murmurhash128(oakBuffer, Integer.BYTES, test.length, 0, actual);
        Assert.assertEquals(expected, actual);
    }
}
