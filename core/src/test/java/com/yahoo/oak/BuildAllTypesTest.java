/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.common.floatnum.OakFloatComparator;
import com.yahoo.oak.common.floatnum.OakFloatSerializer;
import com.yahoo.oak.common.integer.OakIntComparator;
import com.yahoo.oak.common.integer.OakIntSerializer;
import com.yahoo.oak.common.string.OakStringSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BuildAllTypesTest {

    private static final int MEBIBYTE = 1024 * 1024;

    @Test
    public void testBuildIntInt() {
        OakIntSerializer intSerializer = new OakIntSerializer();
        OakIntSerializer intSerializer2 = new OakIntSerializer();
        OakIntComparator intComparator = new OakIntComparator();

        OakMapBuilder<Integer, Integer> builder = new OakMapBuilder<>(intComparator, intSerializer,
                intSerializer2, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Integer, Integer> oak = builder.buildOrderedMap();

        int myKey = 0;
        int myVal = 1;
        oak.put(myKey, myVal);

        Assert.assertEquals((int) oak.get(myKey), myVal);

    }

    @Test
    public void testBuildIntFloat() {
        OakIntSerializer intSerializer = new OakIntSerializer();
        OakFloatSerializer floatSerializer = new OakFloatSerializer();
        OakIntComparator intComparator = new OakIntComparator();

        OakMapBuilder<Integer, Float> builder = new OakMapBuilder<>(intComparator, intSerializer,
                floatSerializer, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Integer, Float> oak = builder.buildOrderedMap();

        int myKey = 0;
        float myVal = (float) 3.14;
        oak.put(myKey, myVal);

        Assert.assertEquals(oak.get(myKey), myVal, 0.0);

    }

    @Test
    public void testBuildFloatString() {
        OakFloatSerializer floatSerializer = new OakFloatSerializer();
        OakStringSerializer stringSerializer = new OakStringSerializer();
        OakFloatComparator floatComparator = new OakFloatComparator();

        OakMapBuilder<Float, String> builder = new OakMapBuilder<>(floatComparator, floatSerializer,
                stringSerializer, Float.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Float, String> oak = builder.buildOrderedMap();

        float myKey = (float) 2.72;
        String myVal = "val";
        oak.put(myKey, myVal);

        Assert.assertEquals(oak.get(myKey), myVal);

    }

    @Test
    public void testBuildByteArray() {
        OakMap<byte[], byte[]> oak = OakCommonBuildersFactory.getDefaultByteArrayBuilder().buildOrderedMap();

        final byte[] key = new byte[] {1, 2, 3};
        oak.put(key, key);

        Assert.assertArrayEquals(key, oak.get(key));
    }

    @Test
    public void testBuildByteBuffer() {
        OakMap<ByteBuffer, ByteBuffer> oak = OakCommonBuildersFactory.getDefaultBufferBuilder(
            10, 10).buildOrderedMap();

        final ByteBuffer key = ByteBuffer.allocate(10);
        for (int i = 0; i < 10; i++) {
            key.put(i, (byte) i);
        }
        oak.put(key, key);

        final ByteBuffer value = oak.get(key);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(key.get(i), value.get(i));
        }
    }

    // TODO add all type combinations

}
