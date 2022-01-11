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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;


@RunWith(Parameterized.class)
public class ConcurrentPutRemoveTest {
    private static final int NUM_THREADS = 1;
    private static final long TIME_LIMIT_IN_SECONDS = 10;

    private static final long DURATION = 1000;

    private static final int K = 1024;
    private static final int NUM_OF_ENTRIES = 10 * K;

    private ExecutorUtils<Void> executor;
    private ConcurrentZCMap<Integer, Integer> oak;

    private AtomicBoolean stop;
    private AtomicInteger[] status;
    private CyclicBarrier barrier;
    private Supplier<ConcurrentZCMap> builder;

    public ConcurrentPutRemoveTest(Supplier<ConcurrentZCMap> supplier) {
        this.builder = supplier;
    }



    @Parameterized.Parameters
    public static Collection parameters() {

        Supplier<ConcurrentZCMap> s1 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder();

            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap> s2 = () -> {
            OakMapBuilder<Integer, Integer> builder =
                    OakCommonBuildersFactory.getDefaultIntBuilder();
            return builder.buildHashMap();
        };
        return Arrays.asList(new Object[][] {
                { s1 },
                { s2 }
        });
    }



    @Before
    public void initStuff() {
        oak = this.builder.get();
        barrier = new CyclicBarrier(NUM_THREADS + 1);
        stop = new AtomicBoolean(false);
        executor = new ExecutorUtils<>(NUM_THREADS);
        status = new AtomicInteger[NUM_OF_ENTRIES];
        for (int i = 0; i < status.length; i++) {
            status[i] = new AtomicInteger(0);
        }
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
        oak.close();
        BlocksPool.clear();
    }

    class RunThread implements Callable<Void> {
        public Void call() throws BrokenBarrierException, InterruptedException {
            barrier.await();

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
            return null;
        }
    }

    @Ignore
    @Test
    public void testMain() throws InterruptedException, ExecutorUtils.ExecutionError, BrokenBarrierException {
        executor.submitTasks(NUM_THREADS, i -> new RunThread());
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
        barrier.await();

        Thread.sleep(DURATION);
        stop.set(true);
        executor.shutdown(TIME_LIMIT_IN_SECONDS);

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
