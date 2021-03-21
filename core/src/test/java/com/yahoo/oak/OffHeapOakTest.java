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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OffHeapOakTest {
    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 31;
    private  ExecutorService executor;
    private CountDownLatch latch;
    private int maxItemsPerChunk = 248;
    private Exception threadException;

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
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            executor.execute(new RunThreads(latch));
        }

        latch.countDown();


        executor.shutdown();
        try {
            if (!executor.awaitTermination(15000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                Assert.fail("should have done all the tasks in time");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Assert.fail("failed to run all the tasks in the executor service");
        }

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

    class RunThreads implements Runnable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                runTest();
            } catch (Exception e) {
                e.printStackTrace();
                threadException = e;
            }
        }

        private void runTest() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

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
