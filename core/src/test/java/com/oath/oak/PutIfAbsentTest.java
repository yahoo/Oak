package com.oath.oak;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class PutIfAbsentTest {



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


    @Test(timeout=10_000)
    public void testConcurrentPutIfAbsent() {

        OakMapBuilder<ThreadKey, ThreadKey> builder = new OakMapBuilder<ThreadKey, ThreadKey>()
                .setChunkMaxItems(100)
                .setChunkBytesPerItem(128)
                .setKeySerializer(new ThreadKeySerializer())
                .setValueSerializer(new ThreadKeySerializer())
                .setComparator(new ThreadKeyComparator())
                .setMinKey(new ThreadKey(0,-1));

        OakMap<ThreadKey, ThreadKey> oak = builder.build();

        CountDownLatch startSignal = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(8);

        List<Future<?>> threads = new ArrayList<>();
        Integer numThreads = 8;
        int numKeys = 100000;

        for (int i = 0; i < numThreads; ++i ) {
            Thread thread = new Thread(() -> {
                try {
                    startSignal.await();
                    for (int j = 0; j < numKeys; ++j) {
                        ThreadKey threadKey = new ThreadKey(Thread.currentThread().getId(), j);
                        oak.putIfAbsent(threadKey, threadKey);
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

        OakIterator<Map.Entry<ThreadKey, ThreadKey>> iterator = oak.entriesIterator();
        int count2 = 0;
        while(iterator.hasNext()) {
            Map.Entry<ThreadKey, ThreadKey> entry = iterator.next();
            ThreadKey key = entry.getKey();
            ThreadKey value = entry.getValue();
            assertEquals(key.threadId, value.threadId);
            assertEquals(key.value, value.value);
            count2++;
        }
        assertEquals(numKeys, count2);
        assertEquals(numKeys, oak.entries());
    }






    private static class ThreadKey {
        private final long threadId;
        private final int value;

        private ThreadKey(long threadId, int value) {
            this.threadId = threadId;
            this.value = value;
        }
    }


    private static class ThreadKeySerializer implements OakSerializer<ThreadKey> {
        @Override
        public void serialize(ThreadKey object, ByteBuffer targetBuffer) {
            targetBuffer.putLong(0, object.threadId);
            targetBuffer.putInt(Long.BYTES, object.value);
        }

        @Override
        public ThreadKey deserialize(ByteBuffer byteBuffer) {
            return new ThreadKey(byteBuffer.getLong(0), byteBuffer.getInt(Long.BYTES));

        }

        @Override
        public int calculateSize(ThreadKey object) {
            return Integer.BYTES + Long.BYTES;
        }
    }

    private static class ThreadKeyComparator implements OakComparator<ThreadKey> {

        @Override
        public int compareKeys(ThreadKey key1, ThreadKey key2) {
            return key1.value - key2.value;
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            return serializedKey1.getInt(serializedKey1.position() + Long.BYTES) -
                    serializedKey2.getInt(serializedKey2.position() + Long.BYTES);
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, ThreadKey key) {
            return serializedKey.getInt(serializedKey.position() + Long.BYTES) - key.value;
        }
    }

}
