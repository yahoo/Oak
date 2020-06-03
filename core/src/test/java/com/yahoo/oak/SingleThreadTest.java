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

import java.util.function.Consumer;

public class SingleThreadTest {

    private OakMap<Integer, Integer> oak;
    private int maxItemsPerChunk = 2048;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(maxItemsPerChunk);
        oak = builder.build();
    }

    @After
    public void finish() {
        oak.close();
    }

    @Test
    public void testPutAndGet() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
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
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
            Assert.assertTrue(oak.zc().putIfAbsent(i, i));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Assert.assertFalse(oak.zc().putIfAbsent(i, i + 1));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testRemoveAndGet() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
    }

    @Test
    public void testGraduallyRemove() {
        Integer value;
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.zc().putIfAbsent(i, i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNotNull(value);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
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
