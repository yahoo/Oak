package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore
public class PutIfAbsentTest {
    private OakMap<Integer, Integer> oak;
    private CountDownLatch startSignal;
    private List<Future<Integer>> threads;
    private final int NUM_THREADS = 16;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder();
        oak = builder.build();
        startSignal = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
    }

    @After
    public void finish() {
        oak.close();
    }


    @Test(timeout = 10_000)
    public void testConcurrentPutOrCompute() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        int numKeys = 100000;

        for (int i = 0; i < NUM_THREADS; ++i) {
            Callable<Integer> operation = () -> {
                int counter = 0;
                try {
                    startSignal.await();

                    for (int j = 0; j < numKeys; ++j) {
                        boolean retval = oak.zc().putIfAbsentComputeIfPresent(j, 1, buffer -> {
                            int currentVal = buffer.getInt(0);
                            buffer.putInt(0, currentVal + 1);
                        });
                        if (retval) counter++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return counter;
            };

            Future<Integer> future = executor.submit(operation);
            threads.add(future);
        }

        startSignal.countDown();
        final int[] returnValues = {0};
        threads.forEach(t -> {
            try {
                returnValues[0] += t.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                fail();
            }
        });

        Iterator<Integer> iterator = oak.values().iterator();
        int count2 = 0;
        while (iterator.hasNext()) {
            Integer value = iterator.next();
            assertEquals((Integer) NUM_THREADS, value);
            count2++;
        }
        assertEquals(count2, numKeys);
        assertEquals(numKeys, oak.size());
        assertEquals(numKeys, returnValues[0]);
    }


    @Test(timeout = 10_000)
    public void testConcurrentPutIfAbsent() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        int numKeys = 100000;

        for (int i = 0; i < NUM_THREADS; ++i) {
            Callable<Integer> operation = () -> {
                int counter = 0;
                try {
                    startSignal.await();

                    for (int j = 0; j < numKeys; ++j) {
                        boolean retval = oak.zc().putIfAbsent(j, j);
                        if (retval) counter++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return counter;
            };

            Future<Integer> future = executor.submit(operation);
            threads.add(future);
        }

        startSignal.countDown();
        final int[] returnValues = {0};
        threads.forEach(t -> {
            try {
                returnValues[0] += t.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                fail();
            }
        });

        Iterator<Map.Entry<Integer, Integer>> iterator = oak.entrySet().iterator();
        int count2 = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
            assertEquals(entry.getKey(), entry.getValue());
            count2++;
        }
        assertEquals(count2, numKeys);
        assertEquals(numKeys, returnValues[0]);
        assertEquals(numKeys, oak.size());
    }
}
