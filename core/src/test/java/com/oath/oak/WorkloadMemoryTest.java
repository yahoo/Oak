package com.oath.oak;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkloadMemoryTest {
    private OakMap<String, String> oak;
    private static final int NUM_THREADS = 1;
    private static final int K = 1024;
    private static final int M = K * K;
    private static final int NUM_OF_ENTRIES = 512 * K;
    private ArrayList<Thread> threads;
    private AtomicBoolean stop;
    private CyclicBarrier barrier;
    private static final int DURATION = 3000;
    private static final String defaultValue = "value";
    private int getPercents = 0;
    private int putNoResizePercents = getPercents + 100;

    @Before
    public void initStuff() {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(100)
                .setChunkBytesPerItem(128)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");

        oak = builder.build();
        barrier = new CyclicBarrier(NUM_THREADS + 1);
        stop = new AtomicBoolean(false);
        threads = new ArrayList<>(NUM_THREADS);
    }

    private String enlargeValue(String value) {
        return value + value;
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
            String value = defaultValue;
            while (!stop.get()) {
                String key = String.valueOf(r.nextInt(NUM_OF_ENTRIES));
                int op = r.nextInt(100);

                if (op < getPercents)
                    oak.zc().get(key);
                else {
                    if (op < putNoResizePercents)
                        oak.zc().put(key, value);
                    else {
                        value = enlargeValue(value);
                        oak.zc().put(key, value);
                    }
                }

            }
        }
    }

    private void printHeapStats(String message) {
        System.gc();
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        System.out.println("\n" + message);
        System.out.println("heap used: " + (float) (heapSize - heapFreeSize) / M + "MB");
        System.out.println("off heap used: " + (float) (oak.getMemoryManager().allocated()) / M + "MB");
    }

    @Test
    public void testMain() throws InterruptedException {
        printHeapStats("Initial stats");
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new RunThread()));
        }
        Random r = new Random();
        for (int i = 0; i < (int) Math.round(NUM_OF_ENTRIES * 0.5); ) {
            String key = String.valueOf(r.nextInt(NUM_OF_ENTRIES));
            if (oak.putIfAbsent(key, defaultValue) == null) {
                i++;
            }
        }

        printHeapStats("After warm-up");

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

        printHeapStats("End of test");
    }

    @Test
    public void allPutTest() throws InterruptedException {
        getPercents = 0;
        putNoResizePercents = 100;
        testMain();
    }

    @Test
    public void halfAndHalfTest() throws InterruptedException {
        getPercents = 50;
        putNoResizePercents = 100;
        testMain();
    }

    @Test
    public void halfAndHalfWithResizeTest() throws InterruptedException {
        getPercents = 50;
        putNoResizePercents = 95;
        testMain();
    }
}
