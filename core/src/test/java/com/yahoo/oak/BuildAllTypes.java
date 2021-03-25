/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.floatnum.OakFloatComparator;
import com.yahoo.oak.common.floatnum.OakFloatSerializer;
import com.yahoo.oak.common.integer.OakIntComparator;
import com.yahoo.oak.common.integer.OakIntSerializer;
import com.yahoo.oak.common.string.OakStringSerializer;

import org.junit.Assert;
import org.junit.Test;

public class BuildAllTypes {

    private static final int MEBIBYTE = 1024 * 1024;

    @Test
    public void testBuildIntInt() {
        OakIntSerializer intSerializer = new OakIntSerializer();
        OakIntSerializer intSerializer2 = new OakIntSerializer();
        OakIntComparator intComparator = new OakIntComparator();

        OakMapBuilder<Integer, Integer> builder = new OakMapBuilder<Integer, Integer>(intComparator, intSerializer,
                intSerializer2, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Integer, Integer> oak = builder.build();

        int myKey = 0;
        int myVal = 1;
        oak.put(myKey, myVal);

        Assert.assertTrue(oak.get(myKey) == myVal);

    }

    @Test
    public void testBuildIntFloat() {
        OakIntSerializer intSerializer = new OakIntSerializer();
        OakFloatSerializer floatSerializer = new OakFloatSerializer();
        OakIntComparator intComparator = new OakIntComparator();

        OakMapBuilder<Integer, Float> builder = new OakMapBuilder<Integer, Float>(intComparator, intSerializer,
                floatSerializer, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Integer, Float> oak = builder.build();

        int myKey = 0;
        float myVal = (float) 3.14;
        oak.put(myKey, myVal);

        Assert.assertTrue(oak.get(myKey) == myVal);

    }

    @Test
    public void testBuildFloatString() {
        OakFloatSerializer floatSerializer = new OakFloatSerializer();
        OakStringSerializer stringSerializer = new OakStringSerializer();
        OakFloatComparator floatComparator = new OakFloatComparator();

        OakMapBuilder<Float, String> builder = new OakMapBuilder<Float, String>(floatComparator, floatSerializer,
                stringSerializer, Float.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Float, String> oak = builder.build();

        float myKey = (float) 2.72;
        String myVal = "val";
        oak.put(myKey, myVal);

        Assert.assertTrue(oak.get(myKey).equals(myVal));

    }

    // TODO add all type combinations

}
