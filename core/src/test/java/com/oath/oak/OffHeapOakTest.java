/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class OffHeapOakTest {
    private OakMap<Integer, Integer> oak;
    private final int NUM_THREADS = 31;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<ByteBuffer> emptyComputer;
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
        emptyComputer = oakWBuffer -> {
            return;
        };
    }

    @After
    public void finish() throws Exception{
        oak.close();
    }


    @Test
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            Thread thread = new Thread(new RunThreads(latch));
            threads.add(thread);
            thread.start();
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

            OakIterator<Map.Entry<Integer, Integer>> iter0 = oak.entriesIterator();
            while (iter0.hasNext()) {
                Map.Entry<Integer, Integer> entry = iter0.next();
                if (entry == null) continue;
                assertEquals(
                    "\nOn should be empty: Key " + entry.getKey()
                        + ", Value " + entry.getValue(),
                    0, entry.getValue() - entry.getKey());
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.put(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }

            OakIterator<Map.Entry<Integer, Integer>> iter9 = oak.entriesIterator();
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

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }

            OakIterator<Map.Entry<Integer, Integer>> iter8 = oak.entriesIterator();
            while (iter8.hasNext()) {
                Map.Entry<Integer, Integer> entry = iter8.next();
                if (entry == null) continue;
                assertTrue(entry.getValue() != null);
                assertEquals(
                    "\nAfter second pass of put and remove: Key " + entry.getKey()
                        + ", Value " + entry.getValue(),
                    0, entry.getValue() - entry.getKey());
            }


            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.put(i, i);
            }
        }
    }
}
