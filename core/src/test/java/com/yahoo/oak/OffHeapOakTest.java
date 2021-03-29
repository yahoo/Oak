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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

public class OffHeapOakTest {
    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 31;
    private ExecutorService executor;
    private CountDownLatch latch;
    private final int maxItemsPerChunk = 248;
    private Exception threadException;
    private final long timeLimitInMs = TimeUnit.MILLISECONDS.convert(15000, TimeUnit.MILLISECONDS);

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(maxItemsPerChunk);
        oak = builder.build();
        latch = new CountDownLatch(1);
        executor = Executors.newFixedThreadPool(NUM_THREADS);
        threadException = null;
    }

    @After
    public void finish() {
        oak.close();
    }


    @Test//(timeout = 15000)
    public void testThreads() throws InterruptedException, TimeoutException, ExecutionException {
        List<Future<?>> tasks = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(executor.submit(new RunThreads(latch)));
        }
        latch.countDown();


        ExecutorUtils.shutdownTaskPool(executor, tasks, timeLimitInMs);

        Assert.assertNull(threadException);

        for (Integer i = 0; i < 6 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            Assert.assertNotNull("\n Value NULL for key " + i + "\n", value);
            if (!i.equals(value)) {
                Assert.assertEquals(i, value);
            }
            Assert.assertEquals(i, value);
        }
    }

    class RunThreads implements Callable<Void> {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Void call() throws InterruptedException {
            runTest();
            return null;
        }

        private void runTest() throws InterruptedException {
            latch.await();
            try {
                for (Map.Entry<Integer, Integer> entry : oak.entrySet()) {
                    if (entry == null) {
                        continue;
                    }
                    Assert.assertEquals(
                            "\nOn should be empty: Key " + entry.getKey()
                                    + ", Value " + entry.getValue(),
                            0, entry.getValue() - entry.getKey());
                }
            } catch (NoSuchElementException ignored) {

            }

            // todo - perhaps check with non-zc versions
            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().put(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }
            try {
                for (Map.Entry<Integer, Integer> entry : oak.entrySet()) {
                    if (entry == null) {
                        continue;
                    }
                    Assert.assertNotNull("\nAfter initial pass of put and remove got entry NULL", entry);
                    Assert.assertNotNull("\nAfter initial pass of put and remove got value NULL for key "
                            + entry.getKey(), entry.getValue());
                    Assert.assertEquals(
                            "\nAfter initial pass of put and remove (range 0-"
                                    + (6 * maxItemsPerChunk) + "): Key " + entry.getKey()
                                    + ", Value " + entry.getValue(),
                            0, entry.getValue() - entry.getKey());
                }
            } catch (NoSuchElementException ignored) {

            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }
            try {
                for (Map.Entry<Integer, Integer> entry : oak.entrySet()) {
                    if (entry == null) {
                        continue;
                    }
                    Assert.assertNotNull(entry.getValue());
                    Assert.assertEquals(
                            "\nAfter second pass of put and remove: Key " + entry.getKey()
                                    + ", Value " + entry.getValue(),
                            0, entry.getValue() - entry.getKey());
                }
            } catch (NoSuchElementException ignored) {

            }


            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().put(i, i);
            }
        }
    }
}
