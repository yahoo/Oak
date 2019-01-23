/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ComputeTest {

    private static int NUM_THREADS;

    static OakMap<ByteBuffer, ByteBuffer> oak;
    private static final long K = 1024;

    private static int keySize = 10;
    private static int valSize = (int) Math.round(5 * K);
    private static int numOfEntries;



    static private ArrayList<Thread> threads = new ArrayList<>(NUM_THREADS);
    static private CountDownLatch latch = new CountDownLatch(1);

    public static class ComputeTestKeySerializer implements OakSerializer<ByteBuffer> {

        @Override
        public void serialize(ByteBuffer obj, ByteBuffer targetBuffer) {
            for (int i = 0; i < keySize; i++) {
                targetBuffer.putInt(Integer.BYTES * i, obj.getInt(Integer.BYTES * i));
            }
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer byteBuffer) {
            ByteBuffer key = ByteBuffer.allocate(keySize);
            key.position(0);
            for (int i = 0; i < keySize; i++) {
                key.putInt(Integer.BYTES * i, byteBuffer.getInt(Integer.BYTES * i));
            }
            key.position(0);
            return key;
        }

        @Override
        public int calculateSize(ByteBuffer buff) {
            return keySize * Integer.BYTES;
        }
    }

    public static class ComputeTestValueSerializer implements OakSerializer<ByteBuffer> {

        @Override
        public void serialize(ByteBuffer value, ByteBuffer targetBuffer) {
            for (int i = 0; i < valSize; i++) {
                targetBuffer.putInt(Integer.BYTES * i, value.getInt(Integer.BYTES * i));
            }
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer serializedValue) {
            ByteBuffer value = ByteBuffer.allocate(valSize * Integer.BYTES);
            value.position(0);
            for (int i = 0; i < valSize; i++) {
                value.putInt(Integer.BYTES * i, serializedValue.getInt(Integer.BYTES * i));
            }
            value.position(0);
            return value;
        }

        @Override
        public int calculateSize(ByteBuffer buff) {
            return valSize * Integer.BYTES;
        }
    }

    public static class ComputeTestComparator implements OakComparator<ByteBuffer> {

        @Override
        public int compareKeys(ByteBuffer buff1, ByteBuffer buff2) {
            for (int i = 0; i < keySize; i++) {
                int i1 = buff1.getInt(buff1.position() + Integer.BYTES * i);
                int i2 = buff2.getInt(buff2.position() + Integer.BYTES * i);
                if (i1 > i2)
                    return 1;
                else if (i1 < i2)
                    return -1;
            }
            return 0;
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            return compareKeys(serializedKey1, serializedKey2);
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, ByteBuffer key) {
            return compareKeys(serializedKey, key);
        }
    }

    static Consumer<OakWBuffer> computer = new Consumer<OakWBuffer>() {
        @Override
        public void accept(OakWBuffer oakWBuffer) {
            if (oakWBuffer.getInt(0) == oakWBuffer.getInt(Integer.BYTES * keySize)) {
                return;
            }
            int[] arr = new int[keySize];
            for (int i = 0; i < 50; i++) {
                for (int j = 0; j < keySize; j++) {
                    arr[j] = oakWBuffer.getInt();
                }
                for (int j = 0; j < keySize; j++) {
                    oakWBuffer.putInt(arr[j]);
                }
            }
        }
    };

    static class RunThreads implements Runnable {
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

            ByteBuffer myKey = ByteBuffer.allocate(keySize * Integer.BYTES);
            ByteBuffer myVal = ByteBuffer.allocate(valSize * Integer.BYTES);

            Random r = new Random();

            for (int i = 0; i < 1000000; i++) {
                int k = r.nextInt(numOfEntries);
                int o = r.nextInt(2);
                myKey.putInt(0, k);
                myVal.putInt(0, k);
                if (o % 2 == 0)
                    oak.computeIfPresent(myKey, computer);
                else
                    oak.putIfAbsent(myKey, myVal);

            }

        }
    }

    @Test
    public void testMain() throws InterruptedException {

        ByteBuffer minKey = ByteBuffer.allocate(keySize * Integer.BYTES);
        minKey.position(0);
        for (int i = 0; i < keySize; i++) {
            minKey.putInt(4 * i, Integer.MIN_VALUE);
        }
        minKey.position(0);

        OakMapBuilder builder = new OakMapBuilder()
                .setChunkMaxItems(2048)
                .setChunkBytesPerItem(100)
                .setKeySerializer(new ComputeTestKeySerializer())
                .setValueSerializer(new ComputeTestValueSerializer())
                .setMinKey(minKey)
                .setComparator(new ComputeTestComparator());

        oak = (OakMap<ByteBuffer, ByteBuffer>) builder.build();

        NUM_THREADS = 16;
        numOfEntries = 100;


        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new RunThreads(latch)));
        }

        for (Integer i = 0; i < (int) Math.round(numOfEntries * 0.5); i++) {
            ByteBuffer key = ByteBuffer.allocate(keySize * Integer.BYTES);
            ByteBuffer val = ByteBuffer.allocate(valSize * Integer.BYTES);
            key.putInt(0, i);
            val.putInt(0, i);
            oak.putIfAbsent(key, val);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }

        latch.countDown();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        for (int i = 0; i < numOfEntries; i++) {
            ByteBuffer key = ByteBuffer.allocate(keySize * Integer.BYTES);
            key.putInt(0, i);
            ByteBuffer val = oak.get(key);
            if (val == null) {
                continue;
            }
            assertEquals(i, val.getInt(0));
            int forty = val.getInt((keySize - 1) * Integer.BYTES);
            assertTrue(forty == i || forty ==0);
        }

        oak.close();
    }
}
