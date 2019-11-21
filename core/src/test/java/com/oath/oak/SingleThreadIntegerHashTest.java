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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SingleThreadIntegerHashTest {

    private OakMap<Integer, Integer> oak;
    private int maxItemsPerChunk = 2048;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = IntegerOakMap.getDefaultBuilder()
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
        oak.zc().createImmutableIndex();
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            OakRBuffer bufferValue = oak.zc().get(i);
            Integer value = bufferValue.getInt(0);
            assertEquals(i, value);
        }
        OakRBuffer bufferValue = oak.zc().get(10);
        Integer value = bufferValue.getInt(0);
        assertEquals((Integer) 10, value);
        oak.zc().put(10, 11);
        bufferValue = oak.zc().get(10);
        value = bufferValue.getInt(0);
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
        oak.zc().createImmutableIndex();
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            OakRBuffer bufferValue = oak.zc().get(i);
            value = bufferValue.getInt(0);
            assertEquals(i, value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            assertFalse(oak.zc().putIfAbsent(i, i + 1));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            OakRBuffer bufferValue = oak.zc().get(i);
            value = bufferValue.getInt(0);
            assertEquals(i, value);
        }
    }

    @Test
    public void testRemoveAndGet() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        oak.zc().createImmutableIndex(); // testing creation on empty map
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNull(value);
        }
    }

    @Test
    public void testComputeIf() {
        Integer value;
        Consumer<OakWBuffer> computer = oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0) {
                oakWBuffer.putInt(0, 1);
            }
        };
        Integer key = 0;
        assertFalse(oak.zc().computeIfPresent(key, computer));
        assertTrue(oak.zc().putIfAbsent(key, key));
        OakRBuffer bufferValue = oak.zc().get(key);
        value = bufferValue.getInt(0);
        assertNotNull(value);
        assertEquals(key, value);
        assertTrue(oak.zc().computeIfPresent(key, computer));
        bufferValue = oak.zc().get(key);
        value = bufferValue.getInt(0);
        assertNotNull(value);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.zc().createImmutableIndex();
        oak.zc().put(key, two);
        bufferValue = oak.zc().get(key);
        value = bufferValue.getInt(0);
        assertNotNull(value);
        assertEquals(two, value);
        assertTrue(oak.zc().computeIfPresent(key, computer));
        bufferValue = oak.zc().get(key);
        value = bufferValue.getInt(0);
        assertNotNull(value);
        assertEquals(two, value);
        oak.zc().put(key, key);
        assertTrue(oak.zc().computeIfPresent(key, computer));
        bufferValue = oak.zc().get(key);
        value = bufferValue.getInt(0);
        assertNotNull(value);
        assertEquals((Integer) 1, value);
        oak.zc().remove(key);
        assertFalse(oak.zc().computeIfPresent(key, computer));
    }
}
