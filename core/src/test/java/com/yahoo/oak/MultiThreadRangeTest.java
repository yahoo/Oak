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

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class MultiThreadRangeTest {

    private static final int NUM_THREADS = 31;
    private static final long TIME_LIMIT_IN_SECONDS = 60;
    private static final int MAX_ITEMS_PER_CHUNK = 2048;

    private OakMap<Integer, Integer> oak;
    private ExecutorUtils<Void> executor;

    private CountDownLatch latch;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer>builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oak = builder.buildMap();
        latch = new CountDownLatch(1);
        executor = new ExecutorUtils<>(NUM_THREADS);
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

            Integer from = 10 * MAX_ITEMS_PER_CHUNK;
            try (OakMap<Integer, Integer> tailMap = oak.tailMap(from, true)) {
                Iterator<Integer> valIter = tailMap.values().iterator();
                int i = 0;
                while (valIter.hasNext() && i < 100) {
                    valIter.next();
                    i++;
                }
            }
            return null;
        }
    }

    @Test
    public void testRange() throws ExecutorUtils.ExecutionError {
        executor.submitTasks(NUM_THREADS, i -> new MultiThreadRangeTest.RunThreads(latch));

        // fill
        Random r = new Random();
        for (int i = 5 * MAX_ITEMS_PER_CHUNK; i > 0; ) {
            Integer j = r.nextInt(10 * MAX_ITEMS_PER_CHUNK);
            if (oak.zc().putIfAbsent(j, j)) {
                i--;
            }
        }

        latch.countDown();
        executor.shutdown(TIME_LIMIT_IN_SECONDS);

        int size = 0;
        for (int i = 0; i < 10 * MAX_ITEMS_PER_CHUNK; i++) {
            if (oak.get(i) != null) {
                size++;
            }
        }
        Assert.assertEquals(5 * MAX_ITEMS_PER_CHUNK, size);
    }
}
