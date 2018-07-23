/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OffHeapOakTest {
    private OakMapOffHeapImpl<Integer, Integer> oak;
    private final int NUM_THREADS = 12;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<ByteBuffer> emptyComputer;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = 100;

    @Before
    public void init() {
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = (OakMapOffHeapImpl<Integer, Integer>) builder.build();
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
        emptyComputer = new Consumer<ByteBuffer>() {
            @Override
            public void accept(ByteBuffer byteBuffer) {
                return;
            }
        };
    }

    @Test
    public void testPutIfAbsent() {
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value != null);
            TestCase.assertEquals(i, value);
        }
    }

    @Test
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new OffHeapOakTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        for (Integer i = 0; i < 6 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }
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

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.put(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.putIfAbsent(i, i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.remove(i);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                oak.put(i, i);
            }

            for (int i = 0; i < maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsentComputeIfPresent(i, i, emptyComputer);
                oak.putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPutIfAbsentComputeIfPresentWithValueCreator() {

        Consumer<ByteBuffer> computer = new Consumer<ByteBuffer>() {
            @Override
            public void accept(ByteBuffer byteBuffer) {
                if (byteBuffer.getInt() == 0)
                    byteBuffer.putInt(0, 1);
            }
        };

        Integer key = 0;
        assertFalse(oak.computeIfPresent(key, computer));

        oak.putIfAbsentComputeIfPresent(key, key, computer);
        Integer value = oak.get(key);
        assertTrue(value != null);
        assertEquals(key, value);
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        Integer two = 2;
        oak.put(key, two);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 2, value);
        assertTrue(oak.computeIfPresent(key, computer));
        assertEquals((Integer) 2, value);
        oak.put(key, key);
        oak.putIfAbsentComputeIfPresent(key, key, computer);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        oak.remove(key);
        assertFalse(oak.computeIfPresent(key, computer));
    }

    @Test
    public void testValuesTransformIterator() {
        for (Integer i = 0; i < 100; i++) {
            oak.put(i, i);
        }

        Function<ByteBuffer,Integer> function = new Function<ByteBuffer, Integer>() {
            @Override
            public Integer apply(ByteBuffer byteBuffer) {
                return byteBuffer.getInt();
            }
        };
        Iterator<Integer> iter = oak.valuesTransformIterator(function);

        for (int i = 0; i < 100; i++) {
            assertEquals(i, (int) iter.next());
        }
    }

    @Test
    public void testEntriesTransformIterator() {
        for (int i = 0; i < 100; i++) {
            oak.put(i, i + 1);
        }

        Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer> function = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer>() {
            @Override
            public Integer apply(Map.Entry<ByteBuffer, ByteBuffer> entry) {
                int key = entry.getKey().getInt();
                int value = entry.getValue().getInt();
                return value - key;
            }
        };
        Iterator<Integer> iter = oak.entriesTransformIterator(function);

        for (int i = 0; i < 100; i++) {
            assertEquals(1, (int) iter.next());
        }
    }
}
