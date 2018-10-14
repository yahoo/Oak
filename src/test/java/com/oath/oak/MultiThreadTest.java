/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MultiThreadTest {

    private OakMap<Integer, Integer> oak;
    private final int NUM_THREADS = 20;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = Integer.BYTES;


    @Before
    public void init() {
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = (OakMap<Integer, Integer>) builder.build();
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
        ((OakNativeMemoryAllocator)oak.getMemoryManager().memoryAllocator).stopMemoryReuse();
    }

    @After
    public void finish() throws Exception{
        oak.close();
    }

    class RunThreads implements Runnable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer value;

            for (Integer i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
                value = oak.get(i);
                assertTrue(value == null);
            }
            for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
            }
            for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertTrue(value != null);
                assertEquals(i, value);
            }
            for (Integer i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
                value = oak.get(i);
                assertTrue(value == null);
            }
            for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
            }
            for (Integer i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }
            for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertTrue(value != null);
                assertEquals(i, value);
            }
            for (Integer i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
            }
            for (Integer i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertTrue(value != null);
                assertEquals(i, value);
            }
            for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                value = oak.get(i);
                assertTrue(value == null);
            }
            for (Integer i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }

            Iterator valIter = oak.valuesIterator();
            Integer twiceMaxItemsPerChunk = 2 * maxItemsPerChunk;
            Integer c = (int) Math.round(0.5 * maxItemsPerChunk);
            while (valIter.hasNext() && c < twiceMaxItemsPerChunk) {
                value = oak.get(c);
                assertTrue(value != null);
                assertEquals(c, value);
                assertEquals(c, valIter.next());
                c++;
            }
            assertEquals(twiceMaxItemsPerChunk, c);

            Integer from = 0;
            Integer to = twiceMaxItemsPerChunk;
            OakMap sub = oak.subMap(from, true, to, false);
            valIter = sub.valuesIterator();
            c = (int) Math.round(0.5 * maxItemsPerChunk);
            while (valIter.hasNext()) {
                value = oak.get(c);
                assertTrue(value != null);
                assertEquals(c, value);
                assertEquals(c, valIter.next());
                c++;
            }
            assertEquals(twiceMaxItemsPerChunk, c);

            from = 1;
            to = (int) Math.round(0.5 * maxItemsPerChunk);
            sub = oak.subMap(from, true, to, false);
            valIter = sub.valuesIterator();
            assertFalse(valIter.hasNext());

            from = 4 * maxItemsPerChunk;
            to = 5 * maxItemsPerChunk;
            sub = oak.subMap(from, true, to, false);
            valIter = sub.valuesIterator();
            assertFalse(valIter.hasNext());

            for (int i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer bb1 = ByteBuffer.allocate(4);
                bb1.putInt(i + 1);
                bb1.flip();
                oak.putIfAbsent(i, i + 1);
            }


        }
    }

    @Test
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        for (Integer i = (int) Math.round(0.5 * maxItemsPerChunk); i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }
        for (Integer i = 2 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
            Integer value = oak.get(i);
            assertTrue(value == null);
        }
    }

    class RunThreadsDescend implements Runnable {
        CountDownLatch latch;

        RunThreadsDescend(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer i;
            Integer value = 1;
            Iterator<Integer> iter;

            for (i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
                value = oak.get(i);
                Assert.assertTrue(value != null);
                assertEquals(i, value);
            }

            iter = oak.valuesIterator();
            i = 0;
            while (iter.hasNext() && i < 2 * maxItemsPerChunk) {
                value = iter.next();
                Assert.assertTrue(value != null);
                assertEquals(i, value);
                i++;
            }

            iter = oak.descendingMap().valuesIterator();
            while (iter.hasNext()) {
                value = iter.next();
                if (value == null) {
                    continue;
                }
            }
            assertEquals(0, value.intValue());

            for (i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }
            iter = oak.valuesIterator();
            i = 0;
            while (iter.hasNext()) {
                i = iter.next();
                if (i == null) {
                    continue;
                }
            }
            Assert.assertTrue(i > maxItemsPerChunk);
            iter = oak.descendingMap().valuesIterator();
            while (iter.hasNext()) {
                i = iter.next();
                if (i == null) {
                    continue;
                }
            }

            Consumer<OakWBuffer> computer = new Consumer<OakWBuffer>() {
                @Override
                public void accept(OakWBuffer oakWBuffer) {
                    if (oakWBuffer.getInt(0) == 0) {
                        oakWBuffer.putInt(0, 1);
                    }
                }
            };

            for (i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                oak.computeIfPresent(i, computer);
            }

            for (i = 5 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }

            iter = oak.valuesIterator();
            i = 0;
            while (iter.hasNext()) {
                i = iter.next();
                if (i == null) {
                    continue;
                }
            }

            iter = oak.descendingMap().valuesIterator();
            while (iter.hasNext()) {
                i = iter.next();
                if (i == null) {
                    continue;
                }
            }
            assertTrue(i <= 1);

            for (i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
            }

            for (i = 0; i < maxItemsPerChunk; i++) {
                oak.computeIfPresent(i, computer);
            }

        }
    }

    @Test
    public void testThreadsDescend() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadTest.RunThreadsDescend(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value != null);
            if (i > 0)
                assertEquals(i, value);
        }
    }
}
