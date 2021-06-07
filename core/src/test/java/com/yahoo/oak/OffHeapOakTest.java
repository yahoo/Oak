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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class OffHeapOakTest {
    private static final int NUM_THREADS = 31;
    private static final long TIME_LIMIT_IN_SECONDS = 250;

    private static final int MAX_ITEMS_PER_CHUNK = 248;

    private OakMap<Integer, Integer> oak;
    private ExecutorUtils<Void> executor;
    private CountDownLatch latch;
    private Exception threadException;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oak = builder.buildOrderedMap();
        latch = new CountDownLatch(1);
        executor = new ExecutorUtils<>(NUM_THREADS);
        threadException = null;
    }

    @After
    public void finish() {
        executor.shutdownNow();
        oak.close();
    }


    @Test//(timeout = 15000)
    public void testThreads() throws ExecutorUtils.ExecutionError {
        executor.submitTasks(NUM_THREADS, i -> new RunThreads(latch));
        latch.countDown();
        executor.shutdown(TIME_LIMIT_IN_SECONDS);

        Assert.assertNull(threadException);

        for (Integer i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
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
            for (int i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().put(i, i);
            }

            for (int i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
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
                                    + (6 * MAX_ITEMS_PER_CHUNK) + "): Key " + entry.getKey()
                                    + ", Value " + entry.getValue(),
                            0, entry.getValue() - entry.getKey());
                }
            } catch (NoSuchElementException ignored) {

            }

            for (int i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (int i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
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


            for (int i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().put(i, i);
            }
        }
    }
}
