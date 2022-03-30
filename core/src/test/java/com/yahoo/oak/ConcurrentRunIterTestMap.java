/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.test_utils.ExecutorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentRunIterTestMap {

    private static final int NUM_ADD_THREADS = 2;
    private static final int NUM_ITER_THREADS = 1;
    private static final int ELEMENT_COUNT = 10_000;

    private static final long TIME_LIMIT_IN_SECONDS = 80;

    private static final int MAX_ITEMS_PER_CHUNK = 2048;

    private OakMap<Integer, Integer> oak;
    private ExecutorUtils<Integer> executor;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
            .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oak = builder.buildOrderedMap();
        executor = new ExecutorUtils<>(NUM_ADD_THREADS + NUM_ITER_THREADS);
    }

    @After
    public void finish() {
        executor.shutdownNow();
        oak.close();
        BlocksPool.clear();
    }

    @Test
    public void testThreads() throws ExecutorUtils.ExecutionError {
        final CountDownLatch readyLatch = new CountDownLatch(NUM_ADD_THREADS + NUM_ITER_THREADS + 1);
        final AtomicInteger currentlyRunning = new AtomicInteger(0);

        executor.submitTasks(NUM_ADD_THREADS, threadIdx -> () -> {
            currentlyRunning.getAndIncrement();
            readyLatch.countDown();

            int added = 0;
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                boolean isNew = oak.zc().putIfAbsentComputeIfPresent(i, 1, b -> {
                    b.putInt(0, b.getInt(0) + 1);
                });
                if (isNew) {
                    added += 1;
                }
            }

            currentlyRunning.decrementAndGet();
            return added;
        });

        executor.submitTasks(NUM_ITER_THREADS, i -> () -> {
            readyLatch.countDown();

            int sum = 0;
            while (currentlyRunning.get() > 0) {
                for (Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> a : oak.zc().entryStreamSet()) {
                    sum += a.getValue().getInt(0);
                }
            }
            System.out.println(sum);
            return sum;
        });

        readyLatch.countDown();

        executor.shutdown(TIME_LIMIT_IN_SECONDS);
    }
}
