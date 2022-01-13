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
import java.util.Iterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

public class MultiThreadTestHash {


    private static final int NUM_THREADS = 31;
    private static final long TIME_LIMIT_IN_SECONDS = 80;

    private static final int MAX_ITEMS_PER_CHUNK = 2048;

    private OakHashMap<Integer, Integer> oak;
    private ExecutorUtils<Void> executor;

    private CountDownLatch latch;

    private final int threadRangeWidth = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK);
    private final int halfRangeWidth = threadRangeWidth / 2;
    private final int globalWriteRangeStart = (int) Math.round(0.5 * MAX_ITEMS_PER_CHUNK);
    private final int globalWriteRangeEnd = globalWriteRangeStart + NUM_THREADS * threadRangeWidth;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setOrderedChunkMaxItems(MAX_ITEMS_PER_CHUNK);
        oak = builder.buildHashMap();

        latch = new CountDownLatch(1);
        executor = new ExecutorUtils<>(NUM_THREADS);
    }

    @After
    public void finish() {
        executor.shutdownNow();
        oak.close();
    }


    class RunThreads implements Callable<Void> {
        CountDownLatch latch;
        Integer internalId;

        RunThreads(CountDownLatch latch, int id) {
            this.internalId = id;
            this.latch = latch;
        }

        @Override
        public Void call() throws InterruptedException {
            latch.await();
            Integer value;

            int myRangeStart = internalId * threadRangeWidth + globalWriteRangeStart;
            int myRangeEnd = myRangeStart + threadRangeWidth;

            for (int i = 0; i < globalWriteRangeStart; i++) {
                value = oak.get(i);
                Assert.assertNull(value);
            }
            for (int i = myRangeStart; i < myRangeStart + halfRangeWidth; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (Integer i = globalWriteRangeStart; i < globalWriteRangeEnd; i++) {
                value = oak.get(i);
                if (i >= myRangeStart && i < myRangeStart + halfRangeWidth) {
                    Assert.assertEquals(i, value);
                }
                if (value != null) {
                    Assert.assertEquals(i, value);
                }
            }
            for (int i = 0; i < globalWriteRangeStart; i++) {
                value = oak.get(i);
                Assert.assertNull(value);
            }
            for (int i = myRangeStart + halfRangeWidth; i < myRangeEnd; i++) {
                oak.zc().putIfAbsent(i, i);
            }
            for (int i = myRangeStart + halfRangeWidth; i < myRangeEnd; i++) {
                oak.zc().remove(i);
            }
            for (Integer i = globalWriteRangeStart; i < globalWriteRangeEnd; i++) {
                value = oak.get(i);
                if (i >= myRangeStart && i < myRangeStart + halfRangeWidth) {
                    Assert.assertEquals(i, value);
                } else if (i >= myRangeStart + halfRangeWidth && i < myRangeStart ) {
                    Assert.assertNull(value);
                } else if (value != null) {
                    Assert.assertEquals(i, value);
                }

            }
            for (int i = myRangeStart + halfRangeWidth; i < myRangeEnd; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (Integer i = globalWriteRangeStart; i < globalWriteRangeEnd; i++) {
                value = oak.get(i);
                if (i >= myRangeStart && i < myRangeEnd) {
                    Assert.assertEquals(i, value);
                } else if (value != null) {
                    Assert.assertEquals(i, value);
                }

            }

            for (int i = 0; i < globalWriteRangeStart; i++) {
                value = oak.get(i);
                Assert.assertNull(value);
            }
            for (int i = myRangeStart + halfRangeWidth; i < myRangeEnd; i++) {
                oak.zc().remove(i);
            }

            Iterator<Integer> valIter = oak.values().iterator();
            boolean[] valuesPresent = new boolean[threadRangeWidth];

            while (valIter.hasNext() ) {
                Integer nxtValue = valIter.next();

                if (nxtValue >= myRangeStart && nxtValue < myRangeEnd) {
                    int idxFixed = nxtValue - myRangeStart;

                    Assert.assertFalse("thread ID " + internalId + " value " + nxtValue, valuesPresent[idxFixed]);
                    valuesPresent[idxFixed] = true;
                }
            }

            for (int idx = 0; idx < valuesPresent.length; idx ++) {
                if (idx < halfRangeWidth) {
                    Assert.assertTrue(valuesPresent[idx]);
                } else {
                    Assert.assertFalse(valuesPresent[idx]);
                }
            }


            for (int i = myRangeStart; i < myRangeEnd; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer bb1 = ByteBuffer.allocate(4);
                bb1.putInt(i + 1);
                bb1.flip();
                oak.zc().putIfAbsent(i, i );
            }
            return null;
        }
    }

    @Test
    public void testThreads() throws ExecutorUtils.ExecutionError {
        executor.submitTasks(NUM_THREADS, i -> new MultiThreadTestHash.RunThreads(latch, i));
        latch.countDown();
        executor.shutdown(TIME_LIMIT_IN_SECONDS);

        for (int i = 0; i < globalWriteRangeStart; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
        for (int i = globalWriteRangeStart; i < globalWriteRangeEnd; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value.intValue());
        }
    }

    class RunThreadsDescend implements Callable<Void> {
        CountDownLatch latch;
        CyclicBarrier barrier;
        int internalId;

        RunThreadsDescend(CountDownLatch latch, CyclicBarrier barrier, int id) {
            this.latch = latch;
            this.barrier = barrier;
            this.internalId = id;
        }

        @Override
        public Void call() throws BrokenBarrierException, InterruptedException {
            latch.await();
            Integer i;
            Integer value;
            Iterator<Integer> iter;

            int myRangeStart = internalId * threadRangeWidth + globalWriteRangeStart;
            int myRangeEnd = myRangeStart + threadRangeWidth;


            for (i = myRangeStart; i < myRangeEnd; i++) {
                oak.zc().putIfAbsent(i, i);
                value = oak.get(i);
                Assert.assertEquals(i, value);
            }



            barrier.await();

            for (i = myRangeStart + halfRangeWidth; i < myRangeEnd; i++) {
                oak.zc().remove(i);
            }
            iter = oak.values().iterator();
            i = 0;
            while (iter.hasNext()) {
                iter.next();
                i++;
            }
            int expectedNumberOfEntries = halfRangeWidth * NUM_THREADS;
            Assert.assertTrue(i >= expectedNumberOfEntries);

            Consumer<OakScopedWriteBuffer> computer = oakWBuffer -> {
                if (oakWBuffer.getInt(0) % 4 ==  0) {
                    oakWBuffer.putInt(0, 1);
                }
            };

            for (i = myRangeStart; i < myRangeStart + halfRangeWidth; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            barrier.await();

            for (i = myRangeStart; i < myRangeStart + halfRangeWidth; i++) {
                oak.zc().remove(i);
            }


            for (i = myRangeStart; i < myRangeEnd; i++) {
                oak.zc().putIfAbsent(i, i);
            }

            for (i = myRangeStart; i < myRangeEnd; i++) {
                oak.zc().computeIfPresent(i, computer);
            }

            return null;
        }
    }

    @Test
    public void testThreadsDescend() throws ExecutorUtils.ExecutionError {
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);

        executor.submitTasks(NUM_THREADS, i -> new MultiThreadTestHash.RunThreadsDescend(latch, barrier, i));
        latch.countDown();
        executor.shutdown(TIME_LIMIT_IN_SECONDS);

        for (Integer i = globalWriteRangeStart; i < globalWriteRangeEnd; i++) {
            Integer value = oak.get(i);
            if (i % 4 == 0) {
                Assert.assertEquals(1, value.intValue());
            } else {
                Assert.assertEquals(i, value);
            }
        }
    }

}
