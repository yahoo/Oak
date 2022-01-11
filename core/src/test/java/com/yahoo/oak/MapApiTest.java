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

import java.util.AbstractMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class MapApiTest {

    private OakMap<Integer, Integer> oak;
    private Random r = new Random();

    @Before
    public void init() {
        int maxItemsPerChunk = 2048;
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setOrderedChunkMaxItems(maxItemsPerChunk);
        oak = builder.buildOrderedMap();
    }

    @After
    public void finish() {
        oak.close();
        BlocksPool.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void size() {
        Assert.assertEquals("Initial size should be 0", 0, oak.size());
        oak.put(0, 0);
        Assert.assertEquals("Insertion of new key should increase size by 1", 1, oak.size());
        oak.put(0, 1);
        Assert.assertEquals("Insertion of existing key should not increase size", 1, oak.size());

        oak.remove(0);
        Assert.assertEquals("Removal of existing key should decrease size by 1", 0, oak.size());

        // Should throw UnsupportedOperationException
        oak.subMap(0, 1).size();
    }

    @Test
    public void firstKey() {
        Assert.assertNull("Empty map minimal key should be null", oak.firstKey());

        int key1 = r.nextInt();
        int key2 = r.nextInt();
        int minKey = Math.min(key1, key2);
        oak.put(key1, key1);
        oak.put(key2, key2);
        Assert.assertEquals(String.format("Min key should be %d", minKey), minKey, (int) oak.firstKey());
    }

    @Test
    public void lastKey() {
        Assert.assertNull("Empty map maximal key should be null", oak.lastKey());
        int maxKey = 0;
        oak.put(maxKey, 10);
        Assert.assertEquals(String.format("Max key should be %d", maxKey), maxKey, (int) oak.lastKey());

        maxKey++;
        oak.put(maxKey, 1);
        Assert.assertEquals(String.format("Max key should be %d", maxKey), maxKey, (int) oak.lastKey());
    }


    @Test
    public void lowerKey() {
        oak.put(0, 0);
        oak.put(1, 1);
        oak.put(2, 2);

        Assert.assertEquals(2, (int) oak.lowerKey(3));
        Assert.assertEquals(1, (int) oak.lowerKey(2));
        Assert.assertEquals(0, (int) oak.lowerKey(1));

        Assert.assertNull(oak.lowerKey(0));
        Assert.assertNull(oak.lowerKey(Integer.MIN_VALUE));
    }

    @Test
    public void keySet() {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }
        NavigableSet<Integer> keySet = oak.keySet();

        Assert.assertEquals(numKeys, keySet.size());
        for (int i = 0; i < numKeys; i++) {
            Assert.assertTrue(keySet.contains(i));
        }

        keySet = oak.subMap(3, 5).keySet();
        for (int i = 3; i < 5; i++) {
            Assert.assertTrue(keySet.contains(i));
        }
        Assert.assertFalse(keySet.contains(0));
        Assert.assertFalse(keySet.contains(8));
    }

    @Test
    public void entrySet() {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }

        Set<Map.Entry<Integer, Integer>> entries = oak.entrySet();
        Assert.assertEquals(numKeys, entries.size());
        for (int i = 0; i < numKeys; i++) {
            Assert.assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>(i, i)));
        }

        entries = oak.subMap(3, 5).entrySet();
        for (int i = 3; i < 5; i++) {
            Assert.assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>(i, i)));
        }

        Assert.assertFalse(entries.contains(new AbstractMap.SimpleImmutableEntry<>(0, 0)));
        Assert.assertFalse(entries.contains(new AbstractMap.SimpleImmutableEntry<>(8, 8)));

        entries.forEach(e -> Assert.assertEquals(e.getKey(), e.getValue()));
    }

    @Test
    public void putIfAbsent() {
        Assert.assertNull("putIfAbsent should return null if mapping doesn't exist", oak.putIfAbsent(0, 0));
        Assert.assertEquals("putIfAbsent should insert an item if mapping doesn't exist", 1, oak.size());
        Integer result = oak.putIfAbsent(0, 1);
        Assert.assertNotNull("putIfAbsent should return a non-null value if mapping exists", result);
        Assert.assertEquals("putIfAbsent should return previous value if mapping exists", 0, result.intValue());
        Assert.assertEquals("putIfAbsent should not insert an item if mapping doesn't exist", 1, oak.size());
    }

    @Test
    public void putIfAbsentZC() {
        Assert.assertTrue("putIfAbsentZC should return true if mapping doesn't exist", oak.zc().putIfAbsent(0, 0));
        Assert.assertEquals("putIfAbsent should insert an item if mapping doesn't exist", 1, oak.size());
        Assert.assertFalse("putIfAbsent should return previous value if mapping exists", oak.zc().putIfAbsent(0, 1));
        Assert.assertEquals("putIfAbsent should not insert an item if mapping doesn't exist", 1, oak.size());
    }

    @Test
    public void computeIfPresent() {
        BiFunction<? super Integer, ? super Integer, ? extends Integer> func = (k, v) -> v * 2;

        Assert.assertNull("computeIfPresent should return null if mapping doesn't exist",
                oak.computeIfPresent(0, func));
        oak.put(0, 1);
        Integer result = oak.computeIfPresent(0, func);
        Assert.assertNotNull("computeIfPresent should return a non-null value if mapping exists", result);
        Assert.assertEquals("computeIfPresent should return the new value if mapping exists", 2,
                result.intValue());

        result = oak.get(0);
        Assert.assertNotNull("computeIfPresent should not remove an existing mapping", result);
        Assert.assertEquals("computeIfPresent should modify the existing mapping", 2,
                result.intValue());
    }

    @Test
    public void computeIfPresentZC() {
        Consumer<OakScopedWriteBuffer> func = oakWBuffer -> oakWBuffer.putInt(0, oakWBuffer.getInt(0) * 2);

        Assert.assertFalse("computeIfPresentZC should return false if mapping doesn't exist",
                oak.zc().computeIfPresent(0, func));
        oak.put(0, 1);
        Assert.assertTrue("computeIfPresent should return a non-null value if mapping exists",
                oak.zc().computeIfPresent(0, func));
        Integer result = oak.get(0);
        Assert.assertNotNull("computeIfPresent should not remove an existing mapping", result);
        Assert.assertEquals("computeIfPresent should modify the existing mapping", 2,
                result.intValue());
    }

    @Test
    public void iterTest() {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }

        int from = 4;
        Integer to = 6;

        int expected = from + 1;
        try (OakMap<Integer, Integer> sub = oak.subMap(from, false, to, true)) {
            for (Integer i : sub.values()) {
                Assert.assertEquals(expected, i.intValue());
                expected++;
            }
        }
    }

    @Test
    public void descIterTest() {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }

        Integer from = 4;
        Integer to = 6;

        int expected = to;
        try (OakMap<Integer, Integer> sub = oak.subMap(from, false, to, true).descendingMap()) {
            for (Integer i : sub.values()) {
                Assert.assertEquals(expected, i.intValue());
                expected--;
            }
        }
    }
}
