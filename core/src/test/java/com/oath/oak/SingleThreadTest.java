/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.After;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.function.Consumer;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SingleThreadTest {

    private OakMap<Integer, Integer> oak;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = Integer.BYTES;

    @Before
    public void init() {
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = (OakMap<Integer, Integer>) builder.build();
    }

    @After
    public void finish() throws Exception{
            oak.close();
    }

    @Test
    public void testPutAndGet() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.ZC().put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertEquals(i, value);
        }
        Integer value = oak.get(10);
        assertEquals((Integer) 10, value);
        oak.ZC().put(10, 11);
        value = oak.get(10);
        assertEquals((Integer) 11, value);
    }

    @Test
    public void testPutIfAbsent() {
        Integer value;
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
            assertTrue(oak.ZC().putIfAbsent(i, i));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            assertFalse(oak.ZC().putIfAbsent(i, i + 1));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
    }

    @Test
    public void testRemoveAndGet() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.ZC().remove(i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value == null);
        }
    }

    @Test
    public void testGraduallyRemove() {
        Integer value;
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.ZC().putIfAbsent(i, i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            oak.ZC().remove(i);
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.ZC().remove(i);
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.ZC().remove(i);
        }
        for (Integer i = 0; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            oak.ZC().remove(i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.ZC().put(i, i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            TestCase.assertEquals(i, value);
        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testComputeIf() {
        Integer value;
        Consumer<ByteBuffer> computer = oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0)
                oakWBuffer.putInt(0, 1);
        };
        Integer key = 0;
        assertFalse(oak.ZC().computeIfPresent(key, computer));
        assertTrue(oak.ZC().putIfAbsent(key, key));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(key, value);
        assertTrue(oak.ZC().computeIfPresent(key, computer));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.ZC().put(key, two);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(two, value);
        assertTrue(oak.ZC().computeIfPresent(key, computer));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(two, value);
        oak.ZC().put(key, key);
        assertTrue(oak.ZC().computeIfPresent(key, computer));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        oak.ZC().remove(key);
        assertFalse(oak.ZC().computeIfPresent(key, computer));
    }

    @Test
    public void testCompute() {
        Integer value;
        Consumer<ByteBuffer> computer =  oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0)
                oakWBuffer.putInt(0, 1);
        };
        Integer key = 0;
        assertFalse(oak.ZC().computeIfPresent(key, computer));
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertEquals(key, value);
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.ZC().put(key, two);
        value = oak.get(key);
        assertEquals(two, value);
        assertTrue(oak.ZC().computeIfPresent(key, computer));
        assertEquals(two, value);
        oak.ZC().put(key, key);
        value = oak.get(key);
        assertEquals(key, value);
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertEquals((Integer) 1, value);
        oak.ZC().remove(key);
        assertFalse(oak.ZC().computeIfPresent(key, computer));
    }
}
