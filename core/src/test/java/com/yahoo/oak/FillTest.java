/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class FillTest {

    private static final int NUM_THREADS = 1;

    static OakMap<Integer, Integer> oak;
    private static final long K = 1024;

    private static final int KEY_SIZE = 10;
    private static final int VALUE_SIZE = Math.round(5 * K);
    private static final int NUM_OF_ENTRIES = 100;

    private static ArrayList<Thread> threads = new ArrayList<>(NUM_THREADS);
    private static CountDownLatch latch = new CountDownLatch(1);

    static class RunThreads implements Runnable {
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

            Random r = new Random();

            int id = (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS;
            int amount = (int) Math.round(NUM_OF_ENTRIES * 0.5) / NUM_THREADS;
            int start = id * amount + (int) Math.round(NUM_OF_ENTRIES * 0.5);
            int end = (id + 1) * amount + (int) Math.round(NUM_OF_ENTRIES * 0.5);

            int[] arr = new int[amount];
            for (int i = start, j = 0; i < end; i++, j++) {
                arr[j] = i;
            }

            int usedIdx = arr.length - 1;

            for (int i = 0; i < amount; i++) {

                int nextIdx = r.nextInt(usedIdx + 1);
                Integer next = arr[nextIdx];

                int tmp = arr[usedIdx];
                arr[usedIdx] = next;
                arr[nextIdx] = tmp;
                usedIdx--;

                oak.zc().putIfAbsent(next, next);
            }

            for (int i = end - 1; i >= start; i--) {
                Assert.assertNotEquals(oak.get(i), null);
            }

        }
    }

    @Test
    public void testMain() throws InterruptedException {

        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
            .setChunkMaxItems(2048)
            .setKeySerializer(new OakIntSerializer(KEY_SIZE))
            .setValueSerializer(new OakIntSerializer(VALUE_SIZE));

        oak = builder.build();


        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new RunThreads(latch)));
        }

        for (int i = 0; i < (int) Math.round(NUM_OF_ENTRIES * 0.5); i++) {
            oak.zc().putIfAbsent(i, i);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }

        long startTime = System.currentTimeMillis();

        latch.countDown();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        long stopTime = System.currentTimeMillis();

        for (Integer i = 0; i < NUM_OF_ENTRIES / 2; i++) {
            Integer val = oak.get(i);
            Assert.assertEquals(i, val);
        }

        long elapsedTime = stopTime - startTime;
        oak.close();

    }
}
