/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.doublenum.OakDoubleComparator;
import com.yahoo.oak.common.doublenum.OakDoubleSerializer;
import com.yahoo.oak.common.floatnum.OakFloatComparator;
import com.yahoo.oak.common.floatnum.OakFloatSerializer;
import com.yahoo.oak.common.integer.OakIntComparator;
import com.yahoo.oak.common.integer.OakIntSerializer;
import com.yahoo.oak.common.string.OakStringSerializer;
import org.junit.Assert;
import org.junit.Test;

public class BuildAllTypesTest {

    private static final int MEBIBYTE = 1024 * 1024;

    @Test
    public void testBuildIntInt() {
        OakIntSerializer intSerializer = new OakIntSerializer();
        OakIntSerializer intSerializer2 = new OakIntSerializer();
        OakIntComparator intComparator = new OakIntComparator();

        OakMap<Integer, Integer> oak = new OakMapBuilder<>(intComparator, intSerializer,
                intSerializer2, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE).build();

        int myKey = 0;
        int myVal = 1;
        oak.put(myKey, myVal);

        Assert.assertEquals(myVal, (int) oak.get(myKey));
    }

    @Test
    public void testBuildIntFloat() {
        OakIntSerializer intSerializer = new OakIntSerializer();
        OakFloatSerializer floatSerializer = new OakFloatSerializer();
        OakIntComparator intComparator = new OakIntComparator();

        OakMap<Integer, Float> oak = new OakMapBuilder<>(intComparator, intSerializer,
                floatSerializer, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE).build();

        int myKey = 0;
        float myVal = (float) 3.14;
        oak.put(myKey, myVal);

        Assert.assertEquals(myVal, oak.get(myKey), 0.0);
    }

    @Test
    public void testBuildFloatString() {
        OakFloatSerializer floatSerializer = new OakFloatSerializer();
        OakStringSerializer stringSerializer = new OakStringSerializer();
        OakFloatComparator floatComparator = new OakFloatComparator();

        OakMap<Float, String> oak = new OakMapBuilder<>(floatComparator, floatSerializer,
                stringSerializer, Float.MIN_VALUE).setMemoryCapacity(MEBIBYTE).build();

        float myKey = 2.72f;
        String myVal = "val";
        oak.put(myKey, myVal);

        Assert.assertEquals(myVal, oak.get(myKey));

    }

    @Test
    public void testBuildDoubleString() {
        OakDoubleSerializer doubleSerializer = new OakDoubleSerializer();
        OakDoubleComparator doubleComparator = new OakDoubleComparator();
        OakStringSerializer stringSerializer = new OakStringSerializer();

        OakMap<Double, String> oak = new OakMapBuilder<>(doubleComparator, doubleSerializer,
                stringSerializer, Double.MIN_VALUE).setMemoryCapacity(MEBIBYTE).build();

        double myKey = 2.72d;
        String myVal = "val";
        oak.put(myKey, myVal);

        Assert.assertEquals(myVal, oak.get(myKey));

    }

    // TODO add all type combinations

}
