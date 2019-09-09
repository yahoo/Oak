/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Ignore
public class FillTest {

    private static final int NUM_THREADS = 1;

    static OakMap<Integer, Integer> oak;
    private static final long K = 1024;

    private static final int KEY_SIZE = 10;
    private static final int VALUE_SIZE = Math.round(5 * K);
    private static final int NUM_OF_ENTRIES = 100;

    static private ArrayList<Thread> threads = new ArrayList<>(NUM_THREADS);
    static private CountDownLatch latch = new CountDownLatch(1);

    public static class FillTestKeySerializer implements OakSerializer<Integer> {

        @Override
        public void serialize(Integer key, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), key);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedKey) {
            return serializedKey.getInt(serializedKey.position());
        }

        @Override
        public int calculateSize(Integer key) {
            return KEY_SIZE;
        }
    }

    public static class FillTestValueSerializer implements OakSerializer<Integer> {

        @Override
        public void serialize(Integer value, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), value);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.getInt(serializedValue.position());
        }

        @Override
        public int calculateSize(Integer value) {
            return VALUE_SIZE;
        }
    }

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

            for (Integer i = end - 1; i >= start; i--) {
                assertNotEquals(oak.get(i), null);
            }

        }
    }

    @Test
    public void testMain() throws InterruptedException {

        OakMapBuilder<Integer, Integer> builder = OakMapBuilder
                .getDefaultBuilder()
                .setChunkMaxItems(2048)
                .setChunkBytesPerItem(100)
                .setKeySerializer(new FillTestKeySerializer())
                .setValueSerializer(new FillTestValueSerializer());

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
            assertEquals(i, val);
        }

        long elapsedTime = stopTime - startTime;
        oak.close();

    }
}
