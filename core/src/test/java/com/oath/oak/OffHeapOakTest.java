/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class OffHeapOakTest {
    private OakMap<Integer, Integer> oak;
    private final int NUM_THREADS = 31;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private int maxItemsPerChunk = 248;

    @Before
    public void init() {
        int maxBytesPerChunkItem = 100;
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = builder.build();
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
    }

    @After
    public void finish() {
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
            assertNotNull("\n Value NULL for key " + i + "\n", value);
            if (!i.equals(value)) {
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

            for (Map.Entry<Integer, Integer> entry : oak.entrySet()) {
                if (entry == null) continue;
                assertEquals(
                        "\nOn should be empty: Key " + entry.getKey()
                                + ", Value " + entry.getValue(),
                        0, entry.getValue() - entry.getKey());
            }

            // todo - perhaps check with non-zc versions
            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().put(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            for (Map.Entry<Integer, Integer> entry : oak.entrySet()) {
                if (entry == null) continue;
                assertNotNull("\nAfter initial pass of put and remove got entry NULL", entry);
                assertNotNull("\nAfter initial pass of put and remove got value NULL for key " + entry.getKey(), entry.getValue());
                assertEquals(
                        "\nAfter initial pass of put and remove (range 0-"
                                + (6 * maxItemsPerChunk) + "): Key " + entry.getKey()
                                + ", Value " + entry.getValue(),
                        0, entry.getValue() - entry.getKey());
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            for (Map.Entry<Integer, Integer> entry : oak.entrySet()) {
                if (entry == null) continue;
                assertNotNull(entry.getValue());
                assertEquals(
                        "\nAfter second pass of put and remove: Key " + entry.getKey()
                                + ", Value " + entry.getValue(),
                        0, entry.getValue() - entry.getKey());
            }


            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().put(i, i);
            }
        }
    }
}
