package com.oath.oak;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ThreadIndexCalculatorTest {



    @Test
    public void testReuseIndices() throws InterruptedException {

        Thread[] threads = new Thread[ThreadIndexCalculator.MAX_THREADS];
        Thread[] threadsSecondBatch = new Thread[ThreadIndexCalculator.MAX_THREADS];
        CountDownLatch firstRoundLatch = new CountDownLatch(1);
        CountDownLatch doneFirstRoundLatch = new CountDownLatch(ThreadIndexCalculator.MAX_THREADS);
        CountDownLatch secondRoundLatch = new CountDownLatch(1);
        CountDownLatch doneSecondRoundLatch = new CountDownLatch(ThreadIndexCalculator.MAX_THREADS);
        CountDownLatch firstBatchWait = new CountDownLatch(1);
        CountDownLatch firstBatchRelease = new CountDownLatch(1);

        ThreadIndexCalculator indexCalculator = ThreadIndexCalculator.newInstance();
        ConcurrentSkipListSet<Integer> uniqueIndices = new ConcurrentSkipListSet<>();

        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; ++i){

            Thread thread = new Thread(() -> {
                try {
                    firstRoundLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int index = indexCalculator.getIndex();
                uniqueIndices.add(index);
                doneFirstRoundLatch.countDown();
                try {
                    secondRoundLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                assertEquals(index, indexCalculator.getIndex());
                indexCalculator.releaseIndex();

                index = indexCalculator.getIndex();
                uniqueIndices.add(index);
                doneSecondRoundLatch.countDown();



                try {
                    firstBatchRelease.await();
                    indexCalculator.releaseIndex();
                    firstBatchWait.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            threads[i] = thread;
            thread.start();
        }

        firstRoundLatch.countDown();
        doneFirstRoundLatch.await();
        assertEquals(ThreadIndexCalculator.MAX_THREADS, uniqueIndices.size());
        uniqueIndices.clear();

        secondRoundLatch.countDown();
        doneSecondRoundLatch.await();
        assertEquals(ThreadIndexCalculator.MAX_THREADS, uniqueIndices.size());
        uniqueIndices.clear();
        firstBatchRelease.countDown();

        CountDownLatch secondBatchStart = new CountDownLatch(1);
        CountDownLatch doneSecondBatch = new CountDownLatch(ThreadIndexCalculator.MAX_THREADS);;
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; ++i){

            Thread thread = new Thread(() -> {
                try {
                    secondBatchStart.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                int index = indexCalculator.getIndex();
                uniqueIndices.add(index);
                doneSecondBatch.countDown();
            });
            threadsSecondBatch[i] = thread;
            thread.start();
        }


        secondBatchStart.countDown();
        doneSecondBatch.await();
        assertEquals(ThreadIndexCalculator.MAX_THREADS, uniqueIndices.size());


        firstBatchWait.countDown();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            threads[i].join();
            threadsSecondBatch[i].join();
        }
    }

    @Test(timeout = 10000)
    public void testThreadIDCollision() throws InterruptedException {
        CountDownLatch threadsStart = new CountDownLatch(1);
        CountDownLatch threadsFinished = new CountDownLatch(ThreadIndexCalculator.MAX_THREADS);

        ThreadIndexCalculator indexCalculator = ThreadIndexCalculator.newInstance();
        ConcurrentSkipListSet<Integer> uniqueIndices = new ConcurrentSkipListSet<>();

        List<Thread> threads = new ArrayList<>(ThreadIndexCalculator.MAX_THREADS);

        while (threads.size() < ThreadIndexCalculator.MAX_THREADS) {

            Thread thread = new Thread(() -> {
                try {
                    threadsStart.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int index = indexCalculator.getIndex();
                uniqueIndices.add(index);
                threadsFinished.countDown();
            });
            if (thread.getId() % ThreadIndexCalculator.MAX_THREADS == 0) {
                threads.add(thread);
                thread.start();
            }
        }

        threadsStart.countDown();
        threadsFinished.await();
        assertEquals(ThreadIndexCalculator.MAX_THREADS, uniqueIndices.size());
    }

}
