/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MultiThreadTest {

    private OakMap<Integer, Integer> oak;
    private final int NUM_THREADS = 31;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private int maxItemsPerChunk = 2048;


    @Before
    public void init() {
        int maxBytesPerChunkItem = Integer.BYTES;
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
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

            Integer value;

            for (Integer i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
                value = oak.get(i);
                assertNull(value);
            }
            for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertEquals(i, value);
            }
            for (Integer i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
                value = oak.get(i);
                assertNull(value);
            }
            for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }
            for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertEquals(i, value);
            }
            for (Integer i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (Integer i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertEquals(i, value);
            }
            for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertNull(value);
            }
            for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            Iterator valIter = oak.values().iterator();
            Integer twiceMaxItemsPerChunk = 2 * maxItemsPerChunk;
            Integer c = (int) Math.round(0.5 * maxItemsPerChunk);
            while (valIter.hasNext() && c < twiceMaxItemsPerChunk) {
                value = oak.get(c);
                assertEquals(c, value);
                assertEquals(c, valIter.next());
                c++;
            }
            assertEquals(twiceMaxItemsPerChunk, c);

            Integer from = 0;
            Integer to = twiceMaxItemsPerChunk;
            try (OakMap sub = oak.subMap(from, true, to, false)) {
                valIter = sub.values().iterator();
                c = (int) Math.round(0.5 * maxItemsPerChunk);
                while (valIter.hasNext()) {
                    value = oak.get(c);
                    assertEquals(c, value);
                    assertEquals(c, valIter.next());
                    c++;
                }
                assertEquals(twiceMaxItemsPerChunk, c);
            }


            from = 1;
            to = (int) Math.round(0.5 * maxItemsPerChunk);
            try (OakMap sub = oak.subMap(from, true, to, false)) {
                valIter = sub.values().iterator();
                assertFalse(valIter.hasNext());
            }
            from = 4 * maxItemsPerChunk;
            to = 5 * maxItemsPerChunk;
            try (OakMap sub = oak.subMap(from, true, to, false)) {
                valIter = sub.values().iterator();
                assertFalse(valIter.hasNext());
            }


            for (int i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer bb1 = ByteBuffer.allocate(4);
                bb1.putInt(i + 1);
                bb1.flip();
                oak.zc().putIfAbsent(i, i + 1);
            }


        }
    }

    @Test
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        for (Integer i = (int) Math.round(0.5 * maxItemsPerChunk); i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertEquals(i, value);
        }
        for (Integer i = 2 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
            Integer value = oak.get(i);
            assertNull(value);
        }
    }

    class RunThreadsDescend implements Runnable {
        CountDownLatch latch;
        CyclicBarrier barrier;

        RunThreadsDescend(CountDownLatch latch, CyclicBarrier barrier) {
            this.latch = latch;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer i;
            Integer value = 1;
            Iterator<Integer> iter;

            for (i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
                value = oak.get(i);
                //TODO YONIGO - value can be null because of remove
                assertEquals(i, value);
            }

            iter = oak.values().iterator();
            i = 0;
            while (iter.hasNext() && i < 2 * maxItemsPerChunk) {
                value = iter.next();
                assertEquals(i, value);
                i++;
            }

            try(OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
                iter = oakDesc.values().iterator();
                while (iter.hasNext()) {
                    value = iter.next();
                }
                assertEquals(0, value.intValue());
            }

            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            for (i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }
            iter = oak.values().iterator();
            i = 0;
            while (iter.hasNext()) {
                i = iter.next();
            }
            Assert.assertTrue(i > maxItemsPerChunk);

            Consumer<OakWBuffer> computer = oakWBuffer -> {
                if (oakWBuffer.getInt(0) == 0) {
                    oakWBuffer.putInt(0, 1);
                }
            };

            for (i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            for (i = 5 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().remove(i);
            }

            iter = oak.values().iterator();
            i = 0;
            while (iter.hasNext()) {
                i = iter.next();
            }

            try(OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
                iter = oakDesc.values().iterator();
                while (iter.hasNext()) {
                    i = iter.next();
                }
                assertTrue(i <= 1);
            }

            for (i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (i = 0; i < maxItemsPerChunk; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

        }
    }

    @Test
    public void testThreadsDescend() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadTest.RunThreadsDescend(latch, barrier)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNotNull(value);
            if (i > 0)
                assertEquals(i, value);
        }
    }
}
