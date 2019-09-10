/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.*;

public class SingleThreadTest {

    private OakMap<Integer, Integer> oak;
    private int maxItemsPerChunk = 2048;

    @Before
    public void init() {
        int maxBytesPerChunkItem = Integer.BYTES;
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
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
            assertEquals(i, value);
        }
        Integer value = oak.get(10);
        assertEquals((Integer) 10, value);
        oak.zc().put(10, 11);
        value = oak.get(10);
        assertEquals((Integer) 11, value);
    }

    @Test
    public void testPutIfAbsent() {
        Integer value;
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
            assertTrue(oak.zc().putIfAbsent(i, i));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            assertFalse(oak.zc().putIfAbsent(i, i + 1));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
    }

    @Test
    public void testRemoveAndGet() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNull(value);
        }
    }

    @Test
    public void testGraduallyRemove() {
        Integer value;
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.zc().putIfAbsent(i, i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNotNull(value);
            assertEquals(i, value);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            TestCase.assertEquals(i, value);
        }
    }

    @Test
    public void testComputeIf() {
        Integer value;
        Consumer<OakWBuffer> computer = oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0)
                oakWBuffer.putInt(0, 1);
        };
        Integer key = 0;
        assertFalse(oak.zc().computeIfPresent(key, computer));
        assertTrue(oak.zc().putIfAbsent(key, key));
        value = oak.get(key);
        assertNotNull(value);
        assertEquals(key, value);
        assertTrue(oak.zc().computeIfPresent(key, computer));
        value = oak.get(key);
        assertNotNull(value);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.zc().put(key, two);
        value = oak.get(key);
        assertNotNull(value);
        assertEquals(two, value);
        assertTrue(oak.zc().computeIfPresent(key, computer));
        value = oak.get(key);
        assertNotNull(value);
        assertEquals(two, value);
        oak.zc().put(key, key);
        assertTrue(oak.zc().computeIfPresent(key, computer));
        value = oak.get(key);
        assertNotNull(value);
        assertEquals((Integer) 1, value);
        oak.zc().remove(key);
        assertFalse(oak.zc().computeIfPresent(key, computer));
    }

    @Test
    public void testCompute() {
        Integer value;
        Consumer<OakWBuffer> computer =  oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0)
                oakWBuffer.putInt(0, 1);
        };
        Integer key = 0;
        assertFalse(oak.zc().computeIfPresent(key, computer));
        oak.zc().putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertEquals(key, value);
        oak.zc().putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.zc().put(key, two);
        value = oak.get(key);
        assertEquals(two, value);
        assertTrue(oak.zc().computeIfPresent(key, computer));
        assertEquals(two, value);
        oak.zc().put(key, key);
        value = oak.get(key);
        assertEquals(key, value);
        oak.zc().putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertEquals((Integer) 1, value);
        oak.zc().remove(key);
        assertFalse(oak.zc().computeIfPresent(key, computer));
    }
}
