/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentSkipListMap;

public class OrderedChunkSplitTest {
    private static final int MAX_ITEMS_PER_CHUNK = 10;

    OakMap<String, String> oakStr;
    OakMap<Integer, Integer> oakInt;

    @Before
    public void setUp() {
        OakMapBuilder<String, String> builderInt = OakCommonBuildersFactory.getDefaultStringBuilder()
                .setOrderedChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oakStr = builderInt.buildOrderedMap();

        OakMapBuilder<Integer, Integer> builderStr = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setOrderedChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oakInt = builderStr.buildOrderedMap();
    }

    @After
    public void tearDown() {
        oakStr.close();
        oakInt.close();
        BlocksPool.clear();
    }

    @Test
    public void testSplitByCount() throws NoSuchFieldException, IllegalAccessException {
        for (int i = 0; i < MAX_ITEMS_PER_CHUNK + 1; i++) {
            String key = String.format("-%01d", i);
            oakStr.zc().put(key, key);
        }

        Field field = oakStr.getClass().getDeclaredField("internalOakMap");
        field.setAccessible(true);
        InternalOakMap<String, String> internalOakMap = (InternalOakMap<String, String>) field.get(oakStr);

        Field skipListField = internalOakMap.getClass().getDeclaredField("skiplist");
        skipListField.setAccessible(true);
        ConcurrentSkipListMap<Object, OrderedChunk<String, String>> skipList =
            (ConcurrentSkipListMap<Object, OrderedChunk<String,
                        String>>) skipListField.get(internalOakMap);
        Assert.assertTrue(skipList.size() > 1);
    }

    @Test
    public void testSplitByGet() {
        Integer value;
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            oakInt.zc().put(i, i);
        }
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oakInt.get(i);
            Assert.assertEquals(i, value);
        }

        for (Integer i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
            oakInt.zc().remove(i);
        }
        for (Integer i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
            value = oakInt.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oakInt.get(i);
            Assert.assertEquals(i, value);
        }

        for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            oakInt.zc().remove(i);
        }
        for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oakInt.get(i);
            Assert.assertNull(value);
        }
        for (int i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            oakInt.zc().put(i, i);
        }
        for (int i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oakInt.get(i);
            Assert.assertEquals(i, value.intValue());
        }


        for (Integer i = 1; i < (2 * MAX_ITEMS_PER_CHUNK - 1); i++) {
            oakInt.zc().remove(i);
        }

        value = oakInt.get(0);
        Assert.assertEquals(0, value.intValue());

        value = oakInt.get(2 * MAX_ITEMS_PER_CHUNK - 1);
        Assert.assertEquals(2 * MAX_ITEMS_PER_CHUNK - 1, value.intValue());
    }

    // the size of the keys is no longer the requirement for the split
}
