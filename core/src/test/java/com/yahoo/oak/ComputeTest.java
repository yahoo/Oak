/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */


package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.test_utils.ExecutorUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;


public class ComputeTest {

    private static int NUM_THREADS = 16;

    private static OakMap<ByteBuffer, ByteBuffer> oak;
    private static final long K = 1024;

    private static int keySize = 10;
    private static int valSize = Math.round(5 * K);
    private static int numOfEntries;
    private final long timeLimitInMs=TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);
    ExecutorService executor;


    private CountDownLatch latch;



    @Before
    public void setup() {
        executor = Executors.newFixedThreadPool(NUM_THREADS);
        latch = new CountDownLatch(1);
    }

    private static Consumer<OakScopedWriteBuffer> computer = oakWBuffer -> {
        if (oakWBuffer.getInt(0) == oakWBuffer.getInt(Integer.BYTES * keySize)) {
            return;
        }
        int index = 0;
        int[] arr = new int[keySize];
        for (int i = 0; i < 50; i++) {
            for (int j = 0; j < keySize; j++) {
                arr[j] = oakWBuffer.getInt(index);
                index += Integer.BYTES;
            }
            for (int j = 0; j < keySize; j++) {
                oakWBuffer.putInt(index, arr[j]);
                index += Integer.BYTES;
            }
        }
    };

    static class RunThreads implements Callable<Void> {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Void call() throws InterruptedException {
            latch.await();


            ByteBuffer myKey = ByteBuffer.allocate(keySize * Integer.BYTES);
            ByteBuffer myVal = ByteBuffer.allocate(valSize * Integer.BYTES);

            Random r = new Random();

            for (int i = 0; i < 1000000; i++) {
                int k = r.nextInt(numOfEntries);
                int o = r.nextInt(2);
                myKey.putInt(0, k);
                myVal.putInt(0, k);
                if (o % 2 == 0) {
                    oak.zc().computeIfPresent(myKey, computer);
                } else {
                    oak.zc().putIfAbsent(myKey, myVal);
                }

            }
            return null;
        }
    }

    @Test
    public void testMain() throws InterruptedException, TimeoutException, ExecutionException {
        ByteBuffer minKey = ByteBuffer.allocate(keySize * Integer.BYTES);
        minKey.position(0);
        for (int i = 0; i < keySize; i++) {
            minKey.putInt(4 * i, Integer.MIN_VALUE);
        }
        minKey.position(0);

        OakMapBuilder<ByteBuffer, ByteBuffer> builder =
                OakCommonBuildersFactory.getDefaultIntBufferBuilder(keySize, valSize)
                        .setChunkMaxItems(2048);

        oak = builder.build();


        numOfEntries = 100;

        List<Future<?>> tasks=new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(executor.submit(new RunThreads(latch))) ;
        }

        for (int i = 0; i < (int) Math.round(numOfEntries * 0.5); i++) {
            ByteBuffer key = ByteBuffer.allocate(keySize * Integer.BYTES);
            ByteBuffer val = ByteBuffer.allocate(valSize * Integer.BYTES);
            key.putInt(0, i);
            val.putInt(0, i);
            oak.zc().putIfAbsent(key, val);
        }

        latch.countDown();
        ExecutorUtils.shutdownTaskPool(executor, tasks, timeLimitInMs);

        for (int i = 0; i < numOfEntries; i++) {
            ByteBuffer key = ByteBuffer.allocate(keySize * Integer.BYTES);
            key.putInt(0, i);
            ByteBuffer val = oak.get(key);
            if (val == null) {
                continue;
            }
            Assert.assertEquals(i, val.getInt(0));
            int forty = val.getInt((keySize - 1) * Integer.BYTES);
            Assert.assertTrue(forty == i || forty == 0);
        }

        oak.close();
    }
}
