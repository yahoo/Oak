/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentSkipListMap;

public class ChunkSplitTest {
    private static final int MAX_ITEMS_PER_CHUNK = 10;

    @Test
    public void testSplitByCount() throws NoSuchFieldException, IllegalAccessException {
        OakMapBuilder<String, String> builder = OakCommonBuildersFactory.getDefaultStringBuilder()
            .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        OakMap<String, String> oak = builder.build();

        for (int i = 0; i < MAX_ITEMS_PER_CHUNK + 1; i++) {
            String key = String.format("-%01d", i);
            oak.zc().put(key, key);
        }

        Field field = oak.getClass().getDeclaredField("internalOakMap");
        field.setAccessible(true);
        InternalOakMap<String, String> internalOakMap = (InternalOakMap<String, String>) field.get(oak);

        Field skipListField = internalOakMap.getClass().getDeclaredField("skiplist");
        skipListField.setAccessible(true);
        ConcurrentSkipListMap<Object, Chunk<String, String>> skipList = (ConcurrentSkipListMap<Object, Chunk<String,
                String>>) skipListField.get(internalOakMap);
        Assert.assertTrue(skipList.size() > 1);
        oak.close();
    }

    @Test
    public void testSplitByGet() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        OakMap<Integer, Integer> oak = builder.build();
        Integer value;
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }

        for (Integer i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }

        for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (int i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            oak.zc().put(i, i);
        }
        for (int i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value.intValue());
        }


        for (Integer i = 1; i < (2 * MAX_ITEMS_PER_CHUNK - 1); i++) {
            oak.zc().remove(i);
        }

        value = oak.get(0);
        Assert.assertEquals(0, value.intValue());

        value = oak.get(2 * MAX_ITEMS_PER_CHUNK - 1);
        Assert.assertEquals(2 * MAX_ITEMS_PER_CHUNK - 1, value.intValue());
    }

    // the size of the keys is no longer the requirement for the split
}
