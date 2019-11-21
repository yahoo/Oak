/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SingleThreadStringHashTest {

    private OakMap<String, String> oak;
    private int maxItemsPerChunk = 2048;

    private Function<ByteBuffer, String> deserialize = (byteBuffer) -> {
        int size = byteBuffer.getInt(0);
        StringBuilder object = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = byteBuffer.getChar(Integer.BYTES + byteBuffer.position() + i * Character.BYTES);
            object.append(c);
        }
        return object.toString();
    };

    @Before
    public void init() {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
            .setChunkMaxItems(maxItemsPerChunk)
            .setKeySerializer(new StringSerializer())
            .setValueSerializer(new StringSerializer())
            .setComparator(new StringComparator())
            .setMinKey("");
        oak = builder.build();
    }

    @After
    public void finish() {
        oak.close();
    }

    @Test
    public void testPutAndGet() {
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            String key = String.format("-%01d", i);
            oak.zc().put(key, key);
        }
        oak.zc().createImmutableIndex();
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            String key = String.format("-%01d", i);
            OakRBuffer bufferValue = oak.zc().get(key);
            String value = bufferValue.transform(deserialize);
            assertEquals(key, value);
        }
        ///////////////////
        OakRBuffer bufferValue = oak.zc().get(String.format("-%01d", 10));
        String value = bufferValue.transform(deserialize);
        assertEquals(String.format("-%01d", 10), value);
        oak.zc().put(String.format("-%01d", 10), String.format("-%01d", 11));
        bufferValue = oak.zc().get(String.format("-%01d", 10));
        value = bufferValue.transform(deserialize);
        assertEquals(String.format("-%01d", 11), value);
    }

//    @Test
//    public void testPutIfAbsent() {
//        Integer value;
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            value = oak.get(i);
//            assertNull(value);
//            assertTrue(oak.zc().putIfAbsent(i, i));
//        }
//        oak.zc().createImmutableIndex();
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            OakRBuffer bufferValue = oak.zc().get(i);
//            value = bufferValue.getInt(0);
//            assertEquals(i, value);
//        }
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            assertFalse(oak.zc().putIfAbsent(i, i + 1));
//        }
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            OakRBuffer bufferValue = oak.zc().get(i);
//            value = bufferValue.getInt(0);
//            assertEquals(i, value);
//        }
//    }
//
//    @Test
//    public void testRemoveAndGet() {
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            oak.zc().remove(i);
//        }
//        oak.zc().createImmutableIndex(); // testing creation on empty map
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            Integer value = oak.get(i);
//            assertNull(value);
//        }
//    }
//
//    @Test
//    public void testComputeIf() {
//        Integer value;
//        Consumer<OakWBuffer> computer = oakWBuffer -> {
//            if (oakWBuffer.getInt(0) == 0) {
//                oakWBuffer.putInt(0, 1);
//            }
//        };
//        Integer key = 0;
//        assertFalse(oak.zc().computeIfPresent(key, computer));
//        assertTrue(oak.zc().putIfAbsent(key, key));
//        OakRBuffer bufferValue = oak.zc().get(key);
//        value = bufferValue.getInt(0);
//        assertNotNull(value);
//        assertEquals(key, value);
//        assertTrue(oak.zc().computeIfPresent(key, computer));
//        bufferValue = oak.zc().get(key);
//        value = bufferValue.getInt(0);
//        assertNotNull(value);
//        assertEquals((Integer) 1, value);
//        Integer two = 2;
//        oak.zc().createImmutableIndex();
//        oak.zc().put(key, two);
//        bufferValue = oak.zc().get(key);
//        value = bufferValue.getInt(0);
//        assertNotNull(value);
//        assertEquals(two, value);
//        assertTrue(oak.zc().computeIfPresent(key, computer));
//        bufferValue = oak.zc().get(key);
//        value = bufferValue.getInt(0);
//        assertNotNull(value);
//        assertEquals(two, value);
//        oak.zc().put(key, key);
//        assertTrue(oak.zc().computeIfPresent(key, computer));
//        bufferValue = oak.zc().get(key);
//        value = bufferValue.getInt(0);
//        assertNotNull(value);
//        assertEquals((Integer) 1, value);
//        oak.zc().remove(key);
//        assertFalse(oak.zc().computeIfPresent(key, computer));
//    }
}
