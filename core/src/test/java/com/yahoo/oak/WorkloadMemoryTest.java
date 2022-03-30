/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class WorkloadMemoryTest {
    /*-----------------------Constants-------------------------*/
    private static final int K = 1024;
    private static final int M = K * K;
    private static final int NUM_OF_ENTRIES = 200;
    private static final int DURATION = 3000;
    private static final int KEY_SIZE = 100;
    private static final int VALUE_SIZE = 1000;

    /*--------------------Test Variables----------------------*/
    private static ConcurrentZCMap<Integer, Integer> oak;
    private static AtomicBoolean stop;
    private static CyclicBarrier barrier;
    private static int getPercents = 0;
    private static ArrayList<Thread> threads;
    private static final int NUM_THREADS = 1;

    private final Supplier<ConcurrentZCMap<Integer, Integer>> supplier;

    public WorkloadMemoryTest(Supplier<ConcurrentZCMap<Integer, Integer>> supplier) {
        this.supplier = supplier;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {

        Supplier<ConcurrentZCMap<Integer, Integer>> s1 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setChunkMaxItems(100)
                    .setKeySerializer(new OakIntSerializer(KEY_SIZE))
                    .setValueSerializer(new OakIntSerializer(VALUE_SIZE));
            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap<Integer, Integer>> s2 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setChunkMaxItems(100)
                    .setKeySerializer(new OakIntSerializer(KEY_SIZE))
                    .setValueSerializer(new OakIntSerializer(VALUE_SIZE));
            return builder.buildHashMap();
        };
        return Arrays.asList(new Object[][] {
                { s1 },
                { s2 }
        });
    }

    @Before
    public void initStuff() {
        oak = supplier.get();
        barrier = new CyclicBarrier(NUM_THREADS + 1);
        stop = new AtomicBoolean(false);
        threads = new ArrayList<>(NUM_THREADS);
    }

    @After
    public void tearDown() {
        oak.close();
        BlocksPool.clear();
    }

    static class RunThread extends Thread {
        @Override
        public void run() {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            Random r = new Random();
            while (!stop.get()) {
                Integer key = r.nextInt(NUM_OF_ENTRIES);
                int op = r.nextInt(100);

                if (op < getPercents) {
                    oak.zc().get(key);
                } else {
                    oak.zc().put(key, 8);
                }
            }
        }
    }

    private static void printHeapStats(String message) {
        System.gc();
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        System.out.println("\n" + message);
        System.out.println((float) (heapSize - heapFreeSize) / M);
        System.out.println((float) (oak.memorySize()) / M);
    }

    private static void testMain() throws InterruptedException {
        printHeapStats("Initial stats");
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new RunThread()));
        }
        Random r = new Random();
        for (int i = 0; i < (int) Math.round(NUM_OF_ENTRIES * 0.5); ) {
            Integer key = r.nextInt(NUM_OF_ENTRIES);
            if (oak.putIfAbsent(key, 8) == null) {
                i++;
            }
        }

        printHeapStats("After warm-up");

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }

        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }

        Thread.sleep(DURATION);

        stop.set(true);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        printHeapStats("End of test");
    }

    private static void allPutTest() throws InterruptedException {
        getPercents = 0;
        testMain();
    }

    public void halfAndHalfTest() throws InterruptedException {
        getPercents = 50;
        testMain();
    }

    private void onlyPuts() {
        printHeapStats("Initial stats");
        Random r = new Random();
        for (int i = 0; i < (int) Math.round(NUM_OF_ENTRIES * 0.5); ) {
            Integer key = r.nextInt(NUM_OF_ENTRIES);
            if (oak.putIfAbsent(key, 8) == null) {
                i++;
            }
        }
        printHeapStats("End of test");
    }

    @Ignore
    @Test
    public void start() {
        onlyPuts();
    }
}

