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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class MultiThreadComputeTest {

    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 31;
    private ExecutorService executor;
    private CountDownLatch latch;
    private Consumer<OakScopedWriteBuffer> computer;
    private Consumer<OakScopedWriteBuffer> emptyComputer;
    private static final int MAX_ITEMS_PER_CHUNK = 1024;
    private final long timeLimitInMs=TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oak = builder.build();
        latch = new CountDownLatch(1);
        executor = Executors.newFixedThreadPool(NUM_THREADS);
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

    class RunThreads implements Callable<Void> {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Void call() throws InterruptedException {
            latch.await();

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

            return null;
        }
    }

    @Test
    public void testThreadsCompute() throws InterruptedException, TimeoutException, ExecutionException {

        List<Future<?>> tasks=new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(executor.submit(new MultiThreadComputeTest.RunThreads(latch))) ;
        }

        latch.countDown();

        ExecutorUtils.shutdownTaskPool(executor, tasks, timeLimitInMs);


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
