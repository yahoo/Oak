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
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

public class MultiThreadComputeTest {

    private OakMap<Integer, Integer> oak;
    private final int NUM_THREADS = 31;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<OakWBuffer> computer;
    private Consumer<OakWBuffer> emptyComputer;
    private int maxItemsPerChunk = 1024;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = IntegerOakMap.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk);
        oak = builder.build();
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
        computer = oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0) {
                oakWBuffer.putInt(0, 1);
            }
        };

        emptyComputer = oakWBuffer -> {
        };
    }

    @After
    public void finish() {
        oak.close();
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

            for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (Integer i = 0; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            oak.zc().put(1, 2);

            for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            for (int i = 0; i < maxItemsPerChunk; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, computer);
            }

            Integer value = oak.get(0);
            assertNotNull(value);
            assertEquals((Integer) 1, value);

            for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            for (int i = 5 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = 5 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            for (int i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.zc().put(i, i);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            for (int i = 4 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }

        }
    }

    @Test(timeout = 10000)
    public void testThreadsCompute() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadComputeTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNotNull(value);
            if (i == 0) {
                assertEquals((Integer) 1, value);
                continue;
            }
            if (i == 1) {
                assertEquals((Integer) 2, value);
                continue;
            }
            assertEquals(i, value);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNull(value);
        }

        for (Integer i = 4 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertEquals(i, value);
        }

    }


}
