/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OffHeapOakTest {
    private OakMap<Integer, Integer> oak;
    private final int NUM_THREADS = 12;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<OakWBuffer> emptyComputer;
    int maxItemsPerChunk = 248;
    int maxBytesPerChunkItem = 100;

    @Before
    public void init() {
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = (OakMap<Integer, Integer>) builder.build();
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
        emptyComputer = new Consumer<OakWBuffer>() {
            @Override
            public void accept(OakWBuffer oakWBuffer) {
                return;
            }
        };
    }

    @After
    public void finish() throws Exception{
        oak.close();
    }

//    @Test
//    public void testPutIfAbsent() {
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            oak.put(i, i);
//        }
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            Integer value = oak.get(i);
//            assertTrue(value != null);
//            TestCase.assertEquals(i, value);
//        }
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            oak.remove(i);
//        }
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            oak.put(i, i);
//        }
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            Integer value = oak.get(i);
//            assertTrue(value != null);
//            TestCase.assertEquals(i, value);
//        }
//    }

    @Test
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new OffHeapOakTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        for (Integer i = 0; i < 6 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue("\n Value NULL for key " + i + "\n",value != null);
            if (i != value) {
                assertEquals(i, value);
            }
            assertEquals(i, value);
        }
    }

    class RunThreads implements Runnable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            oak.assertIfNotIdle();
            OakCloseableIterator<Map.Entry<Integer, Integer>> iter0 = oak.entriesIterator();
            while (iter0.hasNext()) {
                Map.Entry<Integer, Integer> entry = iter0.next();
                assertTrue(entry.getValue() != null);
                assertEquals(
                    "\nOn should be empty: Key " + entry.getKey()
                        + ", Value " + entry.getValue(),
                    0, entry.getValue() - entry.getKey());
            }
            iter0.close();
            oak.assertIfNotIdle();
            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.put(i, i);
            }
            oak.assertIfNotIdle();
            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }
            oak.assertIfNotIdle();
            OakCloseableIterator<Map.Entry<Integer, Integer>> iter9 = oak.entriesIterator();
            while (iter9.hasNext()) {
                Map.Entry<Integer, Integer> entry = iter9.next();
                if (entry == null) continue;
                assertTrue(
                    "\nAfter initial pass of put and remove got entry NULL",
                    entry != null);
                assertTrue(
                    "\nAfter initial pass of put and remove got value NULL for key "
                        + entry.getKey(), entry.getValue() != null);
                assertEquals(
                    "\nAfter initial pass of put and remove (range 0-"
                        + (6 * maxItemsPerChunk) + "): Key " + entry.getKey()
                        + ", Value " + entry.getValue(),
                    0, entry.getValue() - entry.getKey());
            }
            iter9.close();
            oak.assertIfNotIdle();
            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }

            OakCloseableIterator<Map.Entry<Integer, Integer>> iter8 = oak.entriesIterator();
            while (iter8.hasNext()) {
                Map.Entry<Integer, Integer> entry = iter8.next();
                if (entry == null) continue;
                assertTrue(entry.getValue() != null);
                assertEquals(
                    "\nAfter second pass of put and remove: Key " + entry.getKey()
                        + ", Value " + entry.getValue(),
                    0, entry.getValue() - entry.getKey());
            }
            iter8.close();

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.put(i, i);
            }

//            OakCloseableIterator<Map.Entry<Integer, Integer>> iter1 = oak.entriesIterator();
//            while (iter1.hasNext()) {
//                Map.Entry<Integer, Integer> entry = iter1.next();
//                if (entry == null) continue;
//                assertEquals(
//                    "\nBefore putIfAbsentComputeIfPresent: Key " + entry.getKey()
//                        + ", Value " + entry.getValue(),
//                    0, entry.getValue() - entry.getKey());
//            }
//            iter1.close();
//
//            for (int i = 0; i < maxItemsPerChunk; i++) {
////                ByteBuffer bb = ByteBuffer.allocate(4);
////                bb.putInt(i);
////                bb.flip();
//                oak.putIfAbsentComputeIfPresent(i, i, emptyComputer);
//                oak.putIfAbsentComputeIfPresent(i, i, emptyComputer);
//            }
//
//
//            OakCloseableIterator<Map.Entry<Integer, Integer>> iter2 = oak.entriesIterator();
//            while (iter2.hasNext()) {
//                Map.Entry<Integer, Integer> entry = iter2.next();
//                assertEquals(
//                    "\nAfter putIfAbsentComputeIfPresent: Key " + entry.getKey()
//                        + ", Value " + entry.getValue(),
//                    0, entry.getValue() - entry.getKey());
//            }
//            iter2.close();
        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

//    @Test
//    public void testPutIfAbsentComputeIfPresentWithValueCreator() {
//
//        Consumer<OakWBuffer> computer = new Consumer<OakWBuffer>() {
//            @Override
//            public void accept(OakWBuffer oakWBuffer) {
//                if (oakWBuffer.getInt() == 0)
//                    oakWBuffer.putInt(0, 1);
//            }
//        };
//
//        Integer key = 0;
//        assertFalse(oak.computeIfPresent(key, computer));
//
//        oak.putIfAbsentComputeIfPresent(key, key, computer);
//        Integer value = oak.get(key);
//        assertTrue(value != null);
//        assertEquals(key, value);
//        oak.putIfAbsentComputeIfPresent(key, key, computer);
//        value = oak.get(key);
//        assertTrue(value != null);
//        assertEquals((Integer) 1, value);
//        Integer two = 2;
//        oak.put(key, two);
//        value = oak.get(key);
//        assertTrue(value != null);
//        assertEquals((Integer) 2, value);
//        assertTrue(oak.computeIfPresent(key, computer));
//        assertEquals((Integer) 2, value);
//        oak.put(key, key);
//        oak.putIfAbsentComputeIfPresent(key, key, computer);
//        value = oak.get(key);
//        assertTrue(value != null);
//        assertEquals((Integer) 1, value);
//        oak.remove(key);
//        assertFalse(oak.computeIfPresent(key, computer));
//    }

//    @Test
//    public void testValuesTransformIterator() {
//        for (Integer i = 0; i < 100; i++) {
//            oak.put(i, i);
//        }
//
//        Iterator<Integer> iter = oak.valuesIterator();
//
//        for (int i = 0; i < 100; i++) {
//            assertEquals(i, (int) iter.next());
//        }
//    }

//    @Test
//    public void testEntriesTransformIterator() {
//        for (int i = 0; i < 100; i++) {
//            oak.put(i, i + 1);
//        }
//
//        Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer> function = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer>() {
//            @Override
//            public Integer apply(Map.Entry<ByteBuffer, ByteBuffer> entry) {
//                int key = entry.getKey().getInt();
//                int value = entry.getValue().getInt();
//                return value - key;
//            }
//        };
//        Iterator<Map.Entry<Integer, Integer>> iter = oak.entriesIterator();
//
//        for (int i = 0; i < 100; i++) {
//            Map.Entry<Integer, Integer> entry = iter.next();
//            assertEquals(1, entry.getValue() - entry.getKey());
//        }
//    }
}
