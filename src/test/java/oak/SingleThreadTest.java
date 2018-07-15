/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleThreadTest {

    private OakMapOffHeapImpl<Integer, Integer> oak;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = Integer.BYTES;

    @Before
    public void init() {
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = builder.buildOffHeapOakMap();
    }

    private int countNumOfChunks() {
        Chunk<Integer, Integer> c = oak.skiplist.firstEntry().getValue();
        int counter = 1;
        Chunk<Integer, Integer> next = c.next.getReference();
        assertFalse(c.next.isMarked());
        while (next != null) {
            assertFalse(next.next.isMarked());
            counter++;
            next = next.next.getReference();
        }
        return counter;
    }

    @Test
    public void testPutAndGet() {
        assertEquals(1, countNumOfChunks());
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        assertEquals(4, countNumOfChunks());
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        assertEquals(4, countNumOfChunks());
        Integer value = oak.get(10);
        assertTrue(value != null);
        TestCase.assertEquals((Integer) 10, value);
        oak.put(10, 11);
        value = oak.get(10);
        assertTrue(value != null);
        TestCase.assertEquals((Integer) 11, value);
    }

    @Test
    public void testPutIfAbsent() {
        Integer value;
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
            assertTrue(oak.putIfAbsent(i, i));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            assertFalse(oak.putIfAbsent(i, i + 1));
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
    }

    @Test
    public void testRemoveAndGet() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        assertEquals(1, countNumOfChunks());
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value == null);
        }
        assertEquals(1, countNumOfChunks());
    }

    @Test
    public void testGraduallyRemove() {
        Integer value;
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.putIfAbsent(i, i);
        }
        assertEquals(8, countNumOfChunks());
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            oak.remove(i);
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.remove(i);
            value = oak.get(i);
            assertTrue(value == null);
        }
        Assert.assertTrue(countNumOfChunks() < 8);
        int countNow = countNumOfChunks();
        for (Integer i = maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        for (Integer i = 0; i < 3 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        Assert.assertTrue(countNumOfChunks() < countNow);
        for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        assertEquals(8, countNumOfChunks());
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testComputeIf() {
        Integer value;
        Computer computer = new Computer() {
            @Override
            public void apply(ByteBuffer byteBuffer) {
                if (byteBuffer.getInt() == 0)
                    byteBuffer.putInt(0, 1);
            }
        };
        Integer key = 0;
        assertFalse(oak.computeIfPresent(key, computer));
        assertTrue(oak.putIfAbsent(key, key));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(key, value);
        assertTrue(oak.computeIfPresent(key, computer));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.put(key, two);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(two, value);
        assertTrue(oak.computeIfPresent(key, computer));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(two, value);
        oak.put(key, key);
        assertTrue(oak.computeIfPresent(key, computer));
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        oak.remove(key);
        assertFalse(oak.computeIfPresent(key, computer));
    }

    @Test
    public void testCompute() {
        Integer value;
        Computer computer = new Computer() {
            @Override
            public void apply(ByteBuffer byteBuffer) {
                if (byteBuffer.getInt() == 0)
                    byteBuffer.putInt(0, 1);
            }
        };
        Integer key = 0;
        assertFalse(oak.computeIfPresent(key, computer));
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(key, value);
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.put(key, two);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(two, value);
        assertTrue(oak.computeIfPresent(key, computer));
        assertEquals(two, value);
        oak.put(key, key);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals(key, value);
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        oak.remove(key);
        assertFalse(oak.computeIfPresent(key, computer));
    }
}
