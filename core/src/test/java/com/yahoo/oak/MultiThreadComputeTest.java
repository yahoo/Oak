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

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class MultiThreadComputeTest {

    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 31;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<OakScopedWriteBuffer> computer;
    private Consumer<OakScopedWriteBuffer> emptyComputer;
    private static final int MAX_ITEMS_PER_CHUNK = 1024;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
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

            for (Integer i = 0; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (Integer i = 0; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

            for (int i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            oak.zc().put(1, 2);

            for (int i = 0; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            for (int i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, computer);
            }

            Integer value = oak.get(0);
            Assert.assertNotNull(value);
            Assert.assertEquals((Integer) 1, value);

            for (int i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = 5 * MAX_ITEMS_PER_CHUNK; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = 5 * MAX_ITEMS_PER_CHUNK; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

            for (int i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = 2 * MAX_ITEMS_PER_CHUNK; i < 3 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().put(i, i);
            }

            for (int i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = 4 * MAX_ITEMS_PER_CHUNK; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

        }
    }

    @Test
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
        for (Integer i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNotNull(value);
            if (i == 0) {
                Assert.assertEquals((Integer) 1, value);
                continue;
            }
            if (i == 1) {
                Assert.assertEquals((Integer) 2, value);
                continue;
            }
            Assert.assertEquals(i, value);
        }
        for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 2 * MAX_ITEMS_PER_CHUNK; i < 3 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }

        for (Integer i = 4 * MAX_ITEMS_PER_CHUNK; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }

    }


}
