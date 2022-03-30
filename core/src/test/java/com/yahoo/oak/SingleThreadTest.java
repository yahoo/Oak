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

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class SingleThreadTest {

    private ConcurrentZCMap<Integer, Integer> oak = null;
    private  static final int MAX_ITEMS_PER_ORDERED_CHUNK = 2048;
    private  static final int MAX_ITEMS_PER_HASH_CHUNK = 256;

    private final Supplier<ConcurrentZCMap<Integer, Integer>> supplier;

    public SingleThreadTest(Supplier<ConcurrentZCMap<Integer, Integer>> supplier) {
        this.supplier = supplier;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {

        Supplier<ConcurrentZCMap<Integer, Integer>> s1 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setChunkMaxItems(MAX_ITEMS_PER_ORDERED_CHUNK);
            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap<Integer, Integer>> s2 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder();
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
        BlocksPool.clear();
    }

    @Test
    public void testPutAndGet() {
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        Integer value = oak.get(10);
        Assert.assertEquals((Integer) 10, value);
        oak.zc().put(10, 11);
        value = oak.get(10);
        Assert.assertEquals((Integer) 11, value);
    }

    @Test
    public void testPutIfAbsent() {
        Integer value;
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
            Assert.assertTrue(oak.zc().putIfAbsent(i, i));
        }
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            Assert.assertFalse(oak.zc().putIfAbsent(i, i + 1));
        }
        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testRemoveAndGet() {
        for (int i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            oak.zc().remove(i);
        }
        for (int i = 0; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
    }

    @Test
    public void testGraduallyRemove() {
        Integer value;
        for (Integer i = 0; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        // populate range [0 <-> 4 * MAX_ITEMS_PER_ORDERED_CHUNK)
        for (Integer i = 0; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            oak.zc().putIfAbsent(i, i);
        }
        Assert.assertEquals(oak.size(), 4 * MAX_ITEMS_PER_ORDERED_CHUNK);
        for (Integer i = 0; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        // remove range [2 * MAX_ITEMS_PER_ORDERED_CHUNK <-> 3 * MAX_ITEMS_PER_ORDERED_CHUNK)
        // remaining ranges [0 <-> 2 * MAX_ITEMS_PER_ORDERED_CHUNK)
        //                  [3 * MAX_ITEMS_PER_ORDERED_CHUNK <-> 4 * MAX_ITEMS_PER_ORDERED_CHUNK)
        for (Integer i = 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i < 3 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            oak.zc().remove(i);
            value = oak.get(i);
            Assert.assertNull(value);
        }
        Assert.assertEquals(oak.size(), 3 * MAX_ITEMS_PER_ORDERED_CHUNK);
        // remove range [1 * MAX_ITEMS_PER_ORDERED_CHUNK <-> 2 * MAX_ITEMS_PER_ORDERED_CHUNK)
        // remaining ranges [0 <-> 1 * MAX_ITEMS_PER_ORDERED_CHUNK)
        //                  [3 * MAX_ITEMS_PER_ORDERED_CHUNK <-> 4 * MAX_ITEMS_PER_ORDERED_CHUNK)
        for (Integer i = MAX_ITEMS_PER_ORDERED_CHUNK; i < 2 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            oak.zc().remove(i);
            value = oak.get(i);
            Assert.assertNull(value);
        }
        Assert.assertEquals(oak.size(), 2 * MAX_ITEMS_PER_ORDERED_CHUNK);
        for (Integer i = MAX_ITEMS_PER_ORDERED_CHUNK; i < 3 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 0; i < MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 3 * MAX_ITEMS_PER_ORDERED_CHUNK; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNotNull(value);
            Assert.assertEquals(i, value);
        }
        // remove range [0 <-> 1 * MAX_ITEMS_PER_ORDERED_CHUNK)
        // remaining range [3 * MAX_ITEMS_PER_ORDERED_CHUNK <-> 4 * MAX_ITEMS_PER_ORDERED_CHUNK)
        for (Integer i = 0; i < MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            Assert.assertTrue("Key " + i + " to be removed wasn't found", oak.zc().remove(i));
        }
        Assert.assertEquals(oak.size(), MAX_ITEMS_PER_ORDERED_CHUNK);
        for (Integer i = 0; i < 3 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 3 * MAX_ITEMS_PER_ORDERED_CHUNK; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 3 * MAX_ITEMS_PER_ORDERED_CHUNK; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (int i = 0; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 4 * MAX_ITEMS_PER_ORDERED_CHUNK; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testComputeIf() {
        Integer value;
        Consumer<OakScopedWriteBuffer> computer = oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0) {
                oakWBuffer.putInt(0, 1);
            }
        };
        Integer key = 0;
        Assert.assertFalse(oak.zc().computeIfPresent(key, computer));
        Assert.assertTrue(oak.zc().putIfAbsent(key, key));
        value = oak.get(key);
        Assert.assertNotNull(value);
        Assert.assertEquals(key, value);
        Assert.assertTrue(oak.zc().computeIfPresent(key, computer));
        value = oak.get(key);
        Assert.assertNotNull(value);
        Assert.assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.zc().put(key, two);
        value = oak.get(key);
        Assert.assertNotNull(value);
        Assert.assertEquals(two, value);
        Assert.assertTrue(oak.zc().computeIfPresent(key, computer));
        value = oak.get(key);
        Assert.assertNotNull(value);
        Assert.assertEquals(two, value);
        oak.zc().put(key, key);
        Assert.assertTrue(oak.zc().computeIfPresent(key, computer));
        value = oak.get(key);
        Assert.assertNotNull(value);
        Assert.assertEquals((Integer) 1, value);
        oak.zc().remove(key);
        Assert.assertFalse(oak.zc().computeIfPresent(key, computer));
    }

    @Test
    public void testCompute() {
        Integer value;
        Consumer<OakScopedWriteBuffer> computer = oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0) {
                oakWBuffer.putInt(0, 1);
            }
        };
        Integer key = 0;
        Assert.assertFalse(oak.zc().computeIfPresent(key, computer));
        oak.zc().putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        Assert.assertEquals(key, value);
        oak.zc().putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        Assert.assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.zc().put(key, two);
        value = oak.get(key);
        Assert.assertEquals(two, value);
        Assert.assertTrue(oak.zc().computeIfPresent(key, computer));
        Assert.assertEquals(two, value);
        oak.zc().put(key, key);
        value = oak.get(key);
        Assert.assertEquals(key, value);
        oak.zc().putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        Assert.assertEquals((Integer) 1, value);
        oak.zc().remove(key);
        Assert.assertFalse(oak.zc().computeIfPresent(key, computer));
    }
}
