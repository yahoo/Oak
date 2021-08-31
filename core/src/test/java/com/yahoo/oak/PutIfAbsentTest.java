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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class PutIfAbsentTest {
    private static final int NUM_THREADS = 31;
    private static final long TIME_LIMIT_IN_SECONDS = 200; // was 25, changed for jacoco
    private static final int NUM_KEYS = 100000;

    private ConcurrentZCMap<Integer, Integer> oak;
    private CountDownLatch startSignal;
    private ExecutorUtils<Integer> executor;


    private final Supplier<ConcurrentZCMap<Integer, Integer>> supplier;


    public PutIfAbsentTest(Supplier<ConcurrentZCMap<Integer, Integer>> supplier) {
        this.supplier = supplier;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {

        Supplier<ConcurrentZCMap<Integer, Integer>> s1 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder();
            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap<Integer, Integer>> s2 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder();
            return builder.buildHashMap();
        };
        return Arrays.asList(new Object[][] {
                { s1 }
            // TODO: return when concurrent same key insertion is supported by OakHash's PutIfAbsent
                /*, { s2 }*/
        });
    }




    @Before
    public void init() {
        oak = supplier.get();
        startSignal = new CountDownLatch(1);
        executor = new ExecutorUtils<>(NUM_THREADS);
    }

    @After
    public void finish() {
        executor.shutdownNow();
        oak.close();
    }


    @Test(timeout = TIME_LIMIT_IN_SECONDS * 1000)
    public void testConcurrentPutOrCompute() throws ExecutorUtils.ExecutionError {
        executor.submitTasks(NUM_THREADS, i -> () -> {
            int counter = 0;
            try {
                startSignal.await();

                for (int j = 0; j < NUM_KEYS; ++j) {
                    boolean retVal = oak.zc().putIfAbsentComputeIfPresent(j, 1, buffer -> {
                        int currentVal = buffer.getInt(0);
                        buffer.putInt(0, currentVal + 1);
                    });
                    if (retVal) {
                        counter++;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return counter;
        });

        startSignal.countDown();
        ArrayList<Integer> result = executor.shutdown(TIME_LIMIT_IN_SECONDS);

        Iterator<Integer> iterator = oak.values().iterator();
        int count2 = 0;
        while (iterator.hasNext()) {
            Integer value = iterator.next();
            Assert.assertEquals((Integer) NUM_THREADS, value);
            count2++;
        }
        Assert.assertEquals(count2, NUM_KEYS);
        Assert.assertEquals(NUM_KEYS, oak.size());
        Assert.assertEquals(NUM_KEYS, (int) result.stream().reduce(0, Integer::sum));
    }


    @Test(timeout = TIME_LIMIT_IN_SECONDS * 1000)
    public void testConcurrentPutIfAbsent() throws ExecutorUtils.ExecutionError {
        executor.submitTasks(NUM_THREADS, i -> () -> {
            int counter = 0;
            try {
                startSignal.await();

                for (int j = 0; j < NUM_KEYS; ++j) {
                    boolean retVal = oak.zc().putIfAbsent(j, j);
                    if (retVal) {
                        counter++;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return counter;
        });

        startSignal.countDown();
        ArrayList<Integer> result = executor.shutdown(TIME_LIMIT_IN_SECONDS);

        Iterator<Map.Entry<Integer, Integer>> iterator = oak.entrySet().iterator();
        int count2 = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
            Assert.assertEquals(entry.getKey(), entry.getValue());
            count2++;
        }
        Assert.assertEquals(count2, NUM_KEYS);
        Assert.assertEquals(NUM_KEYS, oak.size());
        Assert.assertEquals(NUM_KEYS, (int) result.stream().reduce(0, Integer::sum));
    }
}
