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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class MultiThreadRangeTest {

    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 31;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private static final int MAX_ITEMS_PER_CHUNK = 2048;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer>builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oak = builder.build();
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
    }

    @After
    public void finish() {
        oak.close();
    }

    class RunThreads implements Runnable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer from = 10 * MAX_ITEMS_PER_CHUNK;
            try (OakMap<Integer, Integer> tailMap = oak.tailMap(from, true)) {
                Iterator valIter = tailMap.values().iterator();
                int i = 0;
                while (valIter.hasNext() && i < 100) {
                    valIter.next();
                    i++;
                }
            }
        }
    }

    @Test
    public void testRange() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadRangeTest.RunThreads(latch)));
        }

        // fill
        Random r = new Random();
        for (int i = 5 * MAX_ITEMS_PER_CHUNK; i > 0; ) {
            Integer j = r.nextInt(10 * MAX_ITEMS_PER_CHUNK);
            if (oak.zc().putIfAbsent(j, j)) {
                i--;
            }
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        int size = 0;
        for (Integer i = 0; i < 10 * MAX_ITEMS_PER_CHUNK; i++) {
            if (oak.get(i) != null) {
                size++;
            }
        }
        Assert.assertEquals(5 * MAX_ITEMS_PER_CHUNK, size);
    }

}
