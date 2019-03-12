package com.oath.oak;

import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class PutIfAbsentComputeIfPresentTest {



    @Test(timeout=10_000)
    public void testConcurrentPutOrCompute() {
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder();
        OakMap<Integer, Integer> oak = builder.build();

        CountDownLatch startSignal = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(4);


        List<Future<?>> threads = new ArrayList<>();
        Integer numThreads = 4;
        int numKeys = 100000;

        for (int i = 0; i < numThreads; ++i ) {
            Thread thread = new Thread(() -> {
                try {
                    startSignal.await();
                    for (int j = 0; j < numKeys; ++j) {
                        oak.putIfAbsentComputeIfPresent(j, 1, buffer -> {
                            int currentVal = buffer.getInt(buffer.position());
                            buffer.putInt(buffer.position(), currentVal + 1);
                        });
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            Future<?> future = executor.submit(thread);
            threads.add(future);
        }

        startSignal.countDown();

        threads.forEach(t -> {
            try {
                t.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                fail();
            }
        });

        OakIterator<Integer> iterator = oak.valuesIterator();
        int count2 = 0;
        while(iterator.hasNext()) {
            Integer value = iterator.next();
            assertEquals(numThreads, value);
            count2++;
        }
        assertEquals(count2, numKeys);
    }
}
