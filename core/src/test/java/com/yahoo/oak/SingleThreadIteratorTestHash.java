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

import java.util.Iterator;
import java.util.Map;

/**
 * Test to verify basic OakHash functionality
 */
public class SingleThreadIteratorTestHash {

    private OakHashMap<Integer, Integer> oak;
    private final int maxItemsPerChunk = 2048;
    private final int iteratorsRange = 10;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setOrderedChunkMaxItems(maxItemsPerChunk);
        oak = builder.buildHashMap();
    }

    @After
    public void finish() {
        oak.close();
        BlocksPool.clear();
    }

    @Test
    public void testIterator() {
        Integer value;

        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }

        Iterator<Integer> valIter = oak.values().iterator();
        Iterator<Map.Entry<Integer, Integer>> entryIter = oak.entrySet().iterator();
        boolean valuesPresent[] = new boolean[2 * maxItemsPerChunk];
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            Assert.assertFalse(valuesPresent[expectedVal]);
            valuesPresent[expectedVal] = true;

            Map.Entry<Integer, Integer> e = entryIter.next();
            Assert.assertEquals(expectedVal, e.getKey());
            Assert.assertEquals(expectedVal, e.getValue());
        }
        for (boolean flag:valuesPresent) {
            Assert.assertTrue(flag);
        }


        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }

        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }


        valIter = oak.values().iterator();
        entryIter = oak.entrySet().iterator();

        valuesPresent = new boolean[2 * maxItemsPerChunk];
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            Assert.assertFalse(valuesPresent[expectedVal]);
            valuesPresent[expectedVal] = true;

            Map.Entry<Integer, Integer> e = entryIter.next();
            Assert.assertEquals(expectedVal, e.getValue());
        }
        for (int index = 0; index < 2 * maxItemsPerChunk; index++) {
            if ( index < maxItemsPerChunk) {
                Assert.assertFalse(valuesPresent[index]);
            } else {
                Assert.assertTrue(valuesPresent[index]);
            }
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }


        for (Integer i = 1; i < (2 * maxItemsPerChunk - 1); i++) {
            oak.zc().remove(i);
        }

        Integer expectedVal = 0;
        value = oak.get(expectedVal);
        Assert.assertEquals(expectedVal, value);

        expectedVal = 2 * maxItemsPerChunk - 1;
        value = oak.get(expectedVal);
        Assert.assertEquals(expectedVal, value);

        valIter = oak.values().iterator();
        Assert.assertTrue(valIter.hasNext());
        Assert.assertEquals((Integer) 0, valIter.next());
        Assert.assertTrue(valIter.hasNext());
        Assert.assertEquals((Integer) (2 * maxItemsPerChunk - 1), valIter.next());
    }
}
