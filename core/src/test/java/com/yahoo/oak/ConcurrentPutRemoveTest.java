/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentPutRemoveTest {
    private static final long DURATION = 1000;
    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 1;
    private static final int K = 1024;
    private static final int NUM_OF_ENTRIES = 10 * K;
    private ExecutorService executor;

    private AtomicBoolean stop;
    private AtomicInteger[] status;
    private CyclicBarrier barrier;

    @Before
    public void initStuff() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder();
        oak = builder.build();
        barrier = new CyclicBarrier(NUM_THREADS + 1);
        stop = new AtomicBoolean(false);
        executor = Executors.newFixedThreadPool(NUM_THREADS);
        status = new AtomicInteger[NUM_OF_ENTRIES];
        for (int i = 0; i < status.length; i++) {
            status[i] = new AtomicInteger(0);
        }
    }

    class RunThread extends Thread {
        @Override
        public void run() {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            Random r = new Random();

            int id = (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS;

            int[] puts = new int[NUM_OF_ENTRIES];
            int[] removes = new int[NUM_OF_ENTRIES];
            while (!stop.get()) {
                int key = r.nextInt(NUM_OF_ENTRIES);
                int op = r.nextInt(2);

                if (op == 0) {
                    puts[key] += (oak.putIfAbsent(key, id) == null) ? 1 : 0;
                } else {
                    removes[key] += (oak.remove(key) != null) ? 1 : 0;
                }
            }
            for (int i = 0; i < NUM_OF_ENTRIES; i++) {
                status[i].addAndGet(puts[i]);
                status[i].addAndGet(-removes[i]);
            }
        }
    }

    @Ignore
    @Test
    public void testMain() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            executor.execute(new RunThread());
        }
        Random r = new Random();
        for (int i = 0; i < (int) Math.round(NUM_OF_ENTRIES * 0.5); ) {
            int key = r.nextInt(NUM_OF_ENTRIES);
            if (oak.putIfAbsent(key, -1) == null) {
                i++;
                status[key].incrementAndGet();
            }
        }

        for (int i = 0; i < NUM_OF_ENTRIES; i++) {
            int old = status[i].get();
            assert old == 0 || old == 1;
            if (old == 0) {
                Assert.assertNull(oak.get(i));
            } else {
                Assert.assertNotNull(oak.get(i));
            }
        }

        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            Assert.fail("got unexpected exception");
        }

        Thread.sleep(DURATION);

        stop.set(true);

        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                Assert.fail("should have done all the tasks in time");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Assert.fail("failed to run all the tasks in the executor service");
        }

        for (int i = 0; i < NUM_OF_ENTRIES; i++) {
            int old = status[i].get();
            assert old == 0 || old == 1;
            if (old == 0) {
                if (oak.get(i) != null) {
                    Assert.assertNull(oak.get(i));
                }
            } else {
                if (oak.get(i) == null) {
                    Assert.assertNotNull(oak.get(i));
                }
            }
        }
    }
}
