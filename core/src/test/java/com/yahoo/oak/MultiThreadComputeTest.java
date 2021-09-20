/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.test_utils.ExecutorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;


@RunWith(Parameterized.class)
public class MultiThreadComputeTest {
    private static final int NUM_THREADS = 31;
    // prolong due to parameterization, and depending on the hash function
    private static final long TIME_LIMIT_IN_SECONDS = 120;
    private static final int MAX_ITEMS_PER_CHUNK = 256; // was 1024

    private ConcurrentZCMap<Integer, Integer> oak;
    private ExecutorUtils<Void> executor;

    private CountDownLatch latch;
    private Consumer<OakScopedWriteBuffer> computer;
    private Consumer<OakScopedWriteBuffer> emptyComputer;

    private Supplier<ConcurrentZCMap> builder;


    public MultiThreadComputeTest(Supplier<ConcurrentZCMap> supplier) {
        this.builder = supplier;

    }

    @Parameterized.Parameters
    public static Collection parameters() {

        Supplier<ConcurrentZCMap> s1 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setOrderedChunkMaxItems(MAX_ITEMS_PER_CHUNK);
            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap> s2 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setHashChunkMaxItems(MAX_ITEMS_PER_CHUNK);
            return builder.buildHashMap();
        };
        return Arrays.asList(new Object[][] {
                { s1 },
                { s2 }
        });
    }





    @Before
    public void init() {

        oak = builder.get();
        latch = new CountDownLatch(1);
        executor = new ExecutorUtils<>(NUM_THREADS);
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
        executor.shutdownNow();
        oak.close();
    }

    class RunThreads implements Callable<Void> {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Void call() throws InterruptedException {
            latch.await();

            // make each thread to start from different start points so there is no simultaneous
            // insertion of the same keys, as this is currently not supported for OakHash
            // TODO: change the contention back
            int threadId = (int) (Thread.currentThread().getId());
            int int2start = (threadId % 32) * (MAX_ITEMS_PER_CHUNK * 20);
            boolean result = false;

            for (Integer i = int2start; i < int2start + 4 * MAX_ITEMS_PER_CHUNK; i++) {
                if (i == int2start) {
                    result = oak.zc().putIfAbsent(i, i);
                    if (!result) {
                        System.out.println("Weird....");
                    }
                } else {
                    oak.zc().putIfAbsent(i, i);
                }
            }

            Integer value = oak.get(int2start);
            if (value == null) {
                System.out.println("Got a null!" + result);
            }
            Assert.assertNotNull(value);

            for (Integer i = int2start; i < int2start + 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

            value = oak.get(int2start);
            if (value == null) {
                System.out.println("Got a null from " + int2start + "!");
            }
            Assert.assertNotNull(value);

            for (int i = int2start + (3 * MAX_ITEMS_PER_CHUNK);
                 i < int2start + (4 * MAX_ITEMS_PER_CHUNK); i++) {
                oak.zc().remove(i);
            }

            value = oak.get(int2start);
            if (value == null) {
                System.out.println("Got a null!");
            }
            Assert.assertNotNull(value);

            oak.zc().put(1, 2);
            oak.zc().put(0, 1);

            for (int i = int2start; i < int2start + (4 * MAX_ITEMS_PER_CHUNK); i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            value = oak.get(int2start);
            if (value == null) {
                System.out.println("Got a null from " + int2start + "!");
            }
            Assert.assertNotNull(value);

            for (int i = int2start; i < int2start + (MAX_ITEMS_PER_CHUNK); i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, computer);
            }

            value = oak.get(int2start);
            if (value == null) {
                System.out.println("Got a null!");
            }
            Assert.assertNotNull(value);
            if (int2start == 0) {
                Assert.assertEquals((Integer) 1, value);
            } else if (int2start == 1) {
                Assert.assertEquals((Integer) 2, value);
            } else {
                Assert.assertEquals((Integer) int2start, value);
            }

            for (int i = int2start + MAX_ITEMS_PER_CHUNK;
                 i < int2start + (2 * MAX_ITEMS_PER_CHUNK); i++) {
                oak.zc().remove(i);
            }

            for (int i = int2start + 5 * MAX_ITEMS_PER_CHUNK;
                 i < int2start + 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = int2start + 5 * MAX_ITEMS_PER_CHUNK;
                 i < int2start + 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = int2start + 3 * MAX_ITEMS_PER_CHUNK;
                 i < int2start + 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = int2start + 3 * MAX_ITEMS_PER_CHUNK;
                 i < int2start + 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

            for (int i = int2start + 3 * MAX_ITEMS_PER_CHUNK;
                 i < int2start + 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = int2start + 2 * MAX_ITEMS_PER_CHUNK;
                 i < int2start + 3 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().put(i, i);
            }

            for (int i = int2start + 3 * MAX_ITEMS_PER_CHUNK;
                 i < int2start + 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = int2start + MAX_ITEMS_PER_CHUNK;
                 i < int2start + 2 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            for (int i = int2start + 4 * MAX_ITEMS_PER_CHUNK;
                 int2start + i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            return null;
        }
    }

    @Test
    public void testThreadsCompute() throws ExecutorUtils.ExecutionError {
        executor.submitTasks(NUM_THREADS, i -> new MultiThreadComputeTest.RunThreads(latch));
        latch.countDown();
        executor.shutdown(TIME_LIMIT_IN_SECONDS);

        for (Integer i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);

            if (value == null) {
                System.out.println("Got a null!");
            }

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
        for (int i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 2 * MAX_ITEMS_PER_CHUNK; i < 3 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (int i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }

        for (Integer i = 4 * MAX_ITEMS_PER_CHUNK; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }

    }


}
