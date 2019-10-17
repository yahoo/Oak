package com.oath.oak;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ConcurrentPutRemoveTest {
    private static final long DURATION = 1000;
    private OakMap<Integer, Integer> oak;
    private static final int NUM_THREADS = 1;
    private static final int K = 1024;
    private static final int NUM_OF_ENTRIES = 10 * K;
    private ArrayList<Thread> threads;
    private AtomicBoolean stop;
    private AtomicInteger[] status;
    private CyclicBarrier barrier;

    @Before
    public void initStuff() {
        OakMapBuilder<Integer, Integer> builder = IntegerOakMap.getDefaultBuilder();
        oak = builder.build();
        barrier = new CyclicBarrier(NUM_THREADS + 1);
        stop = new AtomicBoolean(false);
        threads = new ArrayList<>(NUM_THREADS);
        status = new AtomicInteger[NUM_OF_ENTRIES];
        for (int i = 0; i < status.length; i++) {
            status[i] = new AtomicInteger(0);
        }
    }

    class RunThread extends Thread {
        @Override
        public void run() {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            Random r = new Random();

            int id = (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS;

            int[] puts = new int[NUM_OF_ENTRIES];
            int[] removes = new int[NUM_OF_ENTRIES];
            while (!stop.get()) {
                int key = r.nextInt(NUM_OF_ENTRIES);
                int op = r.nextInt(2);

                if (op == 0) {
                    puts[key] += (oak.putIfAbsent(key, id) == null) ? 1 : 0;
                } else {
                    removes[key] += (oak.remove(key) != null) ? 1 : 0;
                }
            }
            for (int i = 0; i < NUM_OF_ENTRIES; i++) {
                status[i].addAndGet(puts[i]);
                status[i].addAndGet(-removes[i]);
            }
        }
    }

    @Ignore
    @Test
    public void testMain() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new RunThread()));
        }
        Random r = new Random();
        for (int i = 0; i < (int) Math.round(NUM_OF_ENTRIES * 0.5); ) {
            int key = r.nextInt(NUM_OF_ENTRIES);
            if (oak.putIfAbsent(key, -1) == null) {
                i++;
                status[key].incrementAndGet();
            }
        }

        for (int i = 0; i < NUM_OF_ENTRIES; i++) {
            int old = status[i].get();
            assert old == 0 || old == 1;
            if (old == 0) {
                assertNull(oak.get(i));
            } else {
                assertNotNull(oak.get(i));
            }
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }

        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }

        Thread.sleep(DURATION);

        stop.set(true);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        int shouldBeSize = 0;
        for (int i = 0; i < NUM_OF_ENTRIES; i++) {
            int old = status[i].get();
            shouldBeSize += old;
            assert old == 0 || old == 1;
            if (old == 0) {
                if (oak.get(i) != null) {
                    assertNull(oak.get(i));
                }
            } else {
                if (oak.get(i) == null) {
                    assertNotNull(oak.get(i));
                }
            }
        }
        assertEquals(shouldBeSize, oak.size());
    }
}
