/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;

public class FillTest {

    private static int NUM_THREADS;

    static OakMap<Integer, Integer> oak;
    static ConcurrentSkipListMap<ByteBuffer, ByteBuffer> skiplist;
    private static final long K = 1024;

    private static int keySize = 10;
    private static int valSize = (int) Math.round(5 * K);
    private static int numOfEntries;

    static Integer key;
    private static Integer val;

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
            return keySize;
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
            return valSize;
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

            Integer myKey;
            Integer myVal;

            int id = InternalOakMap.getThreadIndex();
            int amount = (int) Math.round(numOfEntries * 0.5) / NUM_THREADS;
            int start = id * amount + (int) Math.round(numOfEntries * 0.5);
            int end = (id + 1) * amount + (int) Math.round(numOfEntries * 0.5);

            int[] arr = new int[amount];
            for (int i = start, j = 0; i < end; i++,j++) {
                arr[j] = i;
            }

            int usedIdx = arr.length-1;

            for (int i = 0; i < amount; i++) {

                int nextIdx = r.nextInt(usedIdx + 1);
                int next = arr[nextIdx];

                int tmp = arr[usedIdx];
                arr[usedIdx] = next;
                arr[nextIdx] = tmp;
                usedIdx--;

                myKey = next;
                myVal = next;
                oak.putIfAbsent(myKey, myVal);
            }

            for (int i = end-1; i >= start; i--) {
                myKey = i;
                if(oak.get(myKey) == null){
                    System.out.println("error");
                }
            }

        }
    }

    @Test
    public void testMain() throws InterruptedException {

        OakMapBuilder builder = OakMapBuilder
                .getDefaultBuilder()
                .setChunkMaxItems(2048)
                .setChunkBytesPerItem(100)
                .setKeySerializer(new FillTestKeySerializer())
                .setValueSerializer(new FillTestValueSerializer());

        oak = (OakMap<Integer, Integer>) builder.build();

        NUM_THREADS = 16;
        numOfEntries = 100;

        key = 0;
        val = 0;

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new RunThreads(latch)));
        }

        for (int i = 0; i < (int) Math.round(numOfEntries * 0.5); i++) {
            key = i;
            val = i;
            oak.putIfAbsent(key, val);
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

        for (Integer i = 0; i < numOfEntries; i++) {
            key = i;
            Integer val = oak.get(key);
            if (val == null) {
                System.out.println("buffer != null i==" + i);
                return;
            }
            if (val != i) {
                System.out.println("buffer.getInt(0) != i i==" + i);
                return;
            }
        }

        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);

        oak.close();

    }
}
