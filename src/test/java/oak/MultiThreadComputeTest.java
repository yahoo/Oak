/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class MultiThreadComputeTest {

    private OakMapOffHeapImpl oak;
    private final int NUM_THREADS = 20;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<WritableOakBuffer> func;
    private Consumer<WritableOakBuffer> emptyFunc;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = 100;

    @Before
    public void init() {
        Comparator<Object> comparator = new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                ByteBuffer bb1 = (ByteBuffer) o1;
                ByteBuffer bb2 = (ByteBuffer) o2;
                int i1 = bb1.getInt(bb1.position());
                int i2 = bb2.getInt(bb2.position());
                if (i1 > i2) {
                    return 1;
                } else if (i1 < i2) {
                    return -1;
                } else {
                    return 0;
                }
            }
        };
        ByteBuffer min = ByteBuffer.allocate(10);
        min.putInt(Integer.MIN_VALUE);
        min.flip();
        oak = new OakMapOffHeapImpl(comparator, min, maxItemsPerChunk, maxBytesPerChunkItem);
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
//        func = buffer -> {
//            if (buffer.getInt(0) == 0) {
//                buffer.putInt(0, 1);
//            }
//        };
        func = buffer -> {
            ByteBuffer bb = buffer.getByteBuffer();
            if (bb.getInt(0) == 0) {
                bb.putInt(0, 1);
            }
        };
        emptyFunc = buffer -> {
            ByteBuffer bb = buffer.getByteBuffer();
        };

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

            ByteBuffer bb;

            for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer finalBb2 = bb;
                oak.putIfAbsentComputeIfPresent(bb, () -> finalBb2, emptyFunc);
            }

            bb = ByteBuffer.allocate(4);
            bb.putInt(0);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            bb.putInt(0, 1);
            ByteBuffer bb2 = ByteBuffer.allocate(4);
            bb2.putInt(2);
            bb2.flip();
            oak.put(bb, bb2);

            for (int i = 0; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.computeIfPresent(bb, func);
            }

            for (int i = 0; i < maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer finalBb = bb;
                oak.putIfAbsentComputeIfPresent(bb, () -> finalBb, func);
            }

            assertEquals(1, buffer.getInt(0));

            for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 5 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (int i = 5 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer finalBb1 = bb;
                oak.putIfAbsentComputeIfPresent(bb, () -> finalBb1, emptyFunc);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }

            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 4 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

        }
    }

    @Test
    public void testThreadsCompute() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadComputeTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        for (int i = 0; i < maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            if (i == 0) {
                assertEquals(1, buffer.getInt(0));
                continue;
            }
            if (i == 1) {
                assertEquals(2, buffer.getInt(0));
                continue;
            }
            assertEquals(i, buffer.getInt(0));
        }
        for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        for (int i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }
        for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }

        for (int i = 4 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }

    }


}
