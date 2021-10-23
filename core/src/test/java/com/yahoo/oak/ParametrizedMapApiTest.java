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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;


@RunWith(Parameterized.class)
public class ParametrizedMapApiTest {
    private ConcurrentZCMap<Integer, Integer> oak;
    private final Random r = new Random();
    private Supplier<ConcurrentZCMap<Integer , Integer>> supplier;

    public ParametrizedMapApiTest(Supplier<ConcurrentZCMap<Integer , Integer>> supplier) {
        this.supplier = supplier;
    }

    @Parameterized.Parameters
    public static Collection parameters() {

        Supplier<ConcurrentZCMap<Integer , Integer>> s1 = () -> {
            int maxItemsPerChunk = 2048;
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setOrderedChunkMaxItems(maxItemsPerChunk);

            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap<Integer , Integer>> s2 = () -> {
            int maxItemsPerChunk = 512;
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setHashChunkMaxItems(maxItemsPerChunk);
            return builder.buildHashMap();
        };
        return Arrays.asList(new Object[][] {
                { s1 },
                { s2 }
        });
    }



    @Before
    public void init() {
        oak = supplier.get();
    }

    @After
    public void finish() {
        oak.close();
    }

    @Test
    public void size() {
        Assert.assertEquals("Initial size should be 0", 0, oak.size());
        oak.put(0, 0);
        Assert.assertEquals("Insertion of new key should increase size by 1", 1, oak.size());
        oak.put(0, 1);
        Assert.assertEquals("Insertion of existing key should not increase size", 1, oak.size());
        oak.remove(0);
        Assert.assertEquals("Removal of existing key should decrease size by 1", 0, oak.size());

    }

    @Test
    public void isEmpty() {
        Assert.assertTrue("New OakMap should be empty", oak.isEmpty());
        oak.put(0, 0);
        Assert.assertFalse("Insertion of new key should make OakMap non-empty", oak.isEmpty());
        oak.remove(0);
        Assert.assertTrue("Removal of all keys should make OakMap empty", oak.isEmpty());
    }


    @Test
    public void put() {
        int initialValue = r.nextInt();
        Integer res = oak.put(0, initialValue);
        Assert.assertNull("Inserting a new key should return null", res);

        res = oak.put(0, initialValue + 1);
        Assert.assertNotNull("Inserting an existing key should return a value", res);
        Assert.assertEquals("Inserting an existing key should return old value", initialValue, (int) res);
    }

    @Test
    public void putZC() {
        int initialValue = r.nextInt();
        oak.zc().put(0, initialValue);
        Assert.assertEquals("zc insertion of new key should increase size by 1", 1, oak.size());

        oak.zc().put(0, initialValue + 1);
        Assert.assertEquals("zc insertion of existing key should not increase size", 1, oak.size());
    }


    @Test
    public void get() {
        int key = r.nextInt();
        int expectedValue = r.nextInt();
        oak.put(key, expectedValue);

        Assert.assertEquals("Looking up an existing key should return the mapped value", expectedValue,
                (int) oak.get(key));
        Assert.assertNull("Looking up a non-existing key should return null", oak.get(key + 1));
    }

    @Test
    public void getZC() {
        int key = r.nextInt();
        int expectedValue = r.nextInt();
        oak.put(key, expectedValue);

        OakUnscopedBuffer result = oak.zc().get(key);
        Assert.assertNotNull("Looking up an existing key should return non-null buffer", result);
        int actualValue = result.getInt(0);
        Assert.assertEquals("Looking up an existing key should return the mapped value", expectedValue, actualValue);
        Assert.assertNull("Looking up a non-existing key should return null", oak.zc().get(key + 1));
    }

    @Test
    public void remove() {
        int key = r.nextInt();
        int expectedValue = r.nextInt();

        /* Remove(K) */
        oak.put(key, expectedValue);
        Integer removed = oak.remove(key);
        Assert.assertNotNull("Removing an existing key should return a value", removed);
        Assert.assertEquals("Remove should return old value", expectedValue, (int) removed);
        Assert.assertNull("Remove should remove the mapping from the map", oak.get(key));
        Assert.assertNull("Removing an non-existing key should return null", oak.remove(key));

        /* Remove(K, V) */
        oak.put(key, expectedValue);
        Assert.assertFalse("Removing a key with non-matching value should return false",
                oak.remove(key, expectedValue + 1));
        Assert.assertTrue("Removing a key with matching value should return true",
                oak.remove(key, expectedValue));
        Assert.assertNull("Removing a key with non-matching value should not remove the value",
                oak.remove(key));
    }

    @Test
    public void removeZC() {
        int key = r.nextInt();
        int expectedValue = r.nextInt();

        /* zc().remove(K) */
        oak.put(key, expectedValue);
        oak.zc().remove(key);
        Assert.assertNull("Remove should remove the mapping from the map", oak.get(key));
    }

    @Test
    public void replace() {
        int key = r.nextInt();
        int val1 = r.nextInt();
        int val2 = r.nextInt();
        oak.put(key, val1);

        /* Replace(K, V) */
        Assert.assertNull("Replacing non-existing key should return null",
            oak.replace(key + 1, val1));
        Integer result = oak.replace(key, val2);
        Assert.assertNotNull("Replacing existing key should return a non-null value",
            result);
        Assert.assertEquals("Replacing existing key should return previous value",
            val1, result.intValue());
        Assert.assertEquals("Replacing existing key should replace the value",
            val2, oak.get(key).intValue());

        /* Replace(K, V, V) */
        Assert.assertFalse("Replacing non-matching value should return false", oak.replace(key, val1, val2));
        Assert.assertTrue("Replacing non-matching value should return true", oak.replace(key, val2, val1));
        Assert.assertEquals("Replacing existing key should replace the value", val1, oak.get(key).intValue());
    }


    @Test
    public void keySet() {
        if (oak instanceof OakHashMap) {
            // TODO: currently iterators are not supported for Hash, remove this later
            return;
        }
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }
        Set<Integer> keySet = oak.keySet();

        Assert.assertEquals(numKeys, keySet.size());
        for (int i = 0; i < numKeys; i++) {
            Assert.assertTrue(keySet.contains(i));
        }
    }

    @Test
    public void iterTest() {
        if (oak instanceof OakHashMap) {
            // TODO: currently iterators are not supported for Hash, remove this later
            return;
        }
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }


        int expected = 0;
        for (Integer i :oak.values()) {
            Assert.assertEquals(expected, i.intValue());
            expected++;
        }
    }

    @Test
    public void entrySet() {
        if (oak instanceof OakHashMap) {
            // TODO: currently iterators are not supported for Hash, remove this later
            return;
        }
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }

        Set<Map.Entry<Integer, Integer>> entries = oak.entrySet();
        Assert.assertEquals(numKeys, entries.size());
        for (int i = 0; i < numKeys; i++) {
            Assert.assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>(i, i)));
        }
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

}
