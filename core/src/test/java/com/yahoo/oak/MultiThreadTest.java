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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class MultiThreadTest {

    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 31;
    private ExecutorService executor;

    private CountDownLatch latch;
    private static final int MAX_ITEMS_PER_CHUNK = 2048;
    private final long timeLimitInMs=TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);


    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oak = builder.build();
        latch = new CountDownLatch(1);
        executor = Executors.newFixedThreadPool(NUM_THREADS);
    }

    @After
    public void finish() {
        oak.close();
    }

    class RunThreads implements Callable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Void call() throws InterruptedException {
            latch.await();
            Integer value;

            for (Integer i = 0; i < (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK); i++) {
                value = oak.get(i);
                Assert.assertNull(value);
            }
            for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
                value = oak.get(i);
                Assert.assertEquals(i, value);
            }
            for (Integer i = 0; i < (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK); i++) {
                value = oak.get(i);
                Assert.assertNull(value);
            }
            for (Integer i = 2 * MAX_ITEMS_PER_CHUNK; i < 3 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (Integer i = 2 * MAX_ITEMS_PER_CHUNK; i < 3 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }
            for (Integer i = MAX_ITEMS_PER_CHUNK; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
                value = oak.get(i);
                Assert.assertEquals(i, value);
            }
            for (Integer i = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK); i < MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (Integer i = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK); i < MAX_ITEMS_PER_CHUNK; i++) {
                value = oak.get(i);
                Assert.assertEquals(i, value);
            }
            for (Integer i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                value = oak.get(i);
                Assert.assertNull(value);
            }
            for (Integer i = 3 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            Iterator valIter = oak.values().iterator();
            Integer twiceMaxItemsPerChunk = 2 * MAX_ITEMS_PER_CHUNK;
            Integer c = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK);
            while (valIter.hasNext() && c < twiceMaxItemsPerChunk) {
                value = oak.get(c);
                Assert.assertEquals(c, value);
                Assert.assertEquals(c, valIter.next());
                c++;
            }
            Assert.assertEquals(twiceMaxItemsPerChunk, c);

            Integer from = 0;
            Integer to = twiceMaxItemsPerChunk;
            try (OakMap sub = oak.subMap(from, true, to, false)) {
                valIter = sub.values().iterator();
                c = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK);
                while (valIter.hasNext()) {
                    value = oak.get(c);
                    Assert.assertEquals(c, value);
                    Assert.assertEquals(c, valIter.next());
                    c++;
                }
                Assert.assertEquals(twiceMaxItemsPerChunk, c);
            }


            from = 1;
            to = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK);
            try (OakMap sub = oak.subMap(from, true, to, false)) {
                valIter = sub.values().iterator();
                Assert.assertFalse(valIter.hasNext());
            }
            from = 4 * MAX_ITEMS_PER_CHUNK;
            to = 5 * MAX_ITEMS_PER_CHUNK;
            try (OakMap sub = oak.subMap(from, true, to, false)) {
                valIter = sub.values().iterator();
                Assert.assertFalse(valIter.hasNext());
            }


            for (int i = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK); i < MAX_ITEMS_PER_CHUNK; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer bb1 = ByteBuffer.allocate(4);
                bb1.putInt(i + 1);
                bb1.flip();
                oak.zc().putIfAbsent(i, i + 1);
            }
            return null;
        }
    }

    @Test
    public void testThreads() throws InterruptedException, TimeoutException, ExecutionException {

        List<Future<?>> tasks=new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(executor.submit(new MultiThreadTest.RunThreads(latch))) ;
        }
        latch.countDown();
        ExecutorUtils.shutdownTaskPool(executor, tasks, timeLimitInMs);

        for (Integer i = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK); i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (Integer i = 2 * MAX_ITEMS_PER_CHUNK; i < 4 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 0; i < (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK); i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
    }

    class RunThreadsDescend implements Callable<Void> {
        CountDownLatch latch;
        CyclicBarrier barrier;

        RunThreadsDescend(CountDownLatch latch, CyclicBarrier barrier) {
            this.latch = latch;
            this.barrier = barrier;
        }

        @Override
        public Void call() throws BrokenBarrierException, InterruptedException {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer i;
            Integer value = 1;
            Iterator<Integer> iter;

            for (i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
                value = oak.get(i);
                Assert.assertEquals(i, value);
            }

            iter = oak.values().iterator();
            i = 0;
            while (iter.hasNext() && i < 2 * MAX_ITEMS_PER_CHUNK) {
                value = iter.next();
                Assert.assertEquals(i, value);
                i++;
            }

            try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
                iter = oakDesc.values().iterator();
                while (iter.hasNext()) {
                    value = iter.next();
                }
                Assert.assertEquals(0, value.intValue());
            }

            barrier.await();

            for (i = 2 * MAX_ITEMS_PER_CHUNK; i < 3 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }
            iter = oak.values().iterator();
            i = 0;
            while (iter.hasNext()) {
                i = iter.next();
            }
            Assert.assertTrue(i > MAX_ITEMS_PER_CHUNK);

            Consumer<OakScopedWriteBuffer> computer = oakWBuffer -> {
                if (oakWBuffer.getInt(0) == 0) {
                    oakWBuffer.putInt(0, 1);
                }
            };

            for (i = 2 * MAX_ITEMS_PER_CHUNK; i < 3 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            for (i = 5 * MAX_ITEMS_PER_CHUNK; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().remove(i);
            }

            iter = oak.values().iterator();
            i = 0;
            while (iter.hasNext()) {

                try {
                    i = iter.next();
                } catch (NoSuchElementException e) {
                    // it is OK to sometimes get NoSuchElement exception here
                    // due to concurrent deletions
                    System.out.println("There was an expected NoSuchElement exception:" + e);
                }

            }

            try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
                iter = oakDesc.values().iterator();
                while (iter.hasNext()) {
                    i = iter.next();
                }
                Assert.assertTrue(i <= 1);
            }

            for (i = 0; i < 6 * MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (i = 0; i < MAX_ITEMS_PER_CHUNK; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            return null;
        }
    }

    @Test
    public void testThreadsDescend() throws InterruptedException, TimeoutException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);

        List<Future<?>> tasks=new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(executor.submit(new MultiThreadTest.RunThreadsDescend(latch, barrier))) ;
        }
        latch.countDown();

        ExecutorUtils.shutdownTaskPool(executor, tasks, timeLimitInMs);

        for (Integer i = 0; i < 2 * MAX_ITEMS_PER_CHUNK; i++) {
            Integer value = oak.get(i);
            Assert.assertNotNull(value);
            if (i > 0) {
                Assert.assertEquals(i, value);
            }
        }
    }
}
