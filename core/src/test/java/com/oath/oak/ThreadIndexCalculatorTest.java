package com.oath.oak;

import org.junit.Test;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;


public class ThreadIndexCalculatorTest {

    private Thread[] threads = new Thread[ThreadIndexCalculator.MAX_THREADS];
    private Thread[] threadsSecondBatch = new Thread[ThreadIndexCalculator.MAX_THREADS];
    private CountDownLatch firstRoundLatch;
    private CountDownLatch doneFirstRoundLatch;
    private CountDownLatch secondRoundLatch;
    private CountDownLatch doneSecondRoundLatch;
    private CountDownLatch firstBatchWait;
    private CountDownLatch firstBatchRelease;
    private CountDownLatch secondBatchStart;
    private CountDownLatch doneSecondBatch;

    private ThreadIndexCalculator indexCalculator = ThreadIndexCalculator.newInstance();
    ConcurrentSkipListSet<Integer> uniqueIndices = new ConcurrentSkipListSet<>();


    @Test
    public void testReuseIndices() throws InterruptedException {

        firstRoundLatch = new CountDownLatch(1);
        secondRoundLatch = new CountDownLatch(1);

        doneFirstRoundLatch = new CountDownLatch(ThreadIndexCalculator.MAX_THREADS);
        doneSecondRoundLatch = new CountDownLatch(ThreadIndexCalculator.MAX_THREADS);

        firstBatchRelease = new CountDownLatch(1);
        firstBatchWait = new CountDownLatch(1);

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

        secondBatchStart = new CountDownLatch(1);
        doneSecondBatch = new CountDownLatch(ThreadIndexCalculator.MAX_THREADS);
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

}
