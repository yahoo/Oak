/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class IteratorModificationTest {

    private OakMap<String, String> oak;
    private static final int ELEMENTS = 1000;
    private static final int KEY_SIZE = 4;
    private static final int VALUE_SIZE = 4;


    private static String generateString(int i, int size) {
        return String.format("%0$" + size + "s", String.valueOf(i));
    }

    @Before
    public void init() {
        OakMapBuilder<String, String> builder = OakCommonBuildersFactory.getDefaultStringBuilder()
                .setChunkMaxItems(100);

        oak = builder.buildOrderedMap();

        for (int i = 0; i < ELEMENTS; i++) {
            String key = generateString(i, KEY_SIZE);
            String val = generateString(i, VALUE_SIZE);
            assert (key.length() == KEY_SIZE);
            assert (val.length() == VALUE_SIZE);
            oak.zc().put(key, val);
        }
    }

    @After
    public void tearDown() {
        oak.close();
        BlocksPool.clear();
    }


    @Test(timeout = 10000)
    public void descendingIterationDuringRebalanceExclude() throws InterruptedException {
        doIterationTest(99,
                false,
                ELEMENTS - 99,
                false,
                true);
    }

    @Test(timeout = 10000)
    public void descendingIterationDuringRebalance() throws InterruptedException {
        doIterationTest(0,
                true,
                ELEMENTS - 1,
                true,
                true);
    }

    @Test(timeout = 1000000)
    public void iterationDuringRebalance() throws InterruptedException {
        doIterationTest(0,
                true,
                ELEMENTS - 1,
                true,
                false);
    }

    @Test(timeout = 10000)
    public void iterationDuringRebalanceExclude() throws InterruptedException {
        doIterationTest(234,
                false,
                ELEMENTS - 342,
                false,
                false);
    }


    private void doIterationTest(int startKey, boolean includeStart, int endKey, boolean includeEnd,
                                 boolean isDescending) throws InterruptedException {

        AtomicBoolean passed = new AtomicBoolean(false);
        AtomicBoolean continueWriting = new AtomicBoolean(true);
        AtomicInteger currentKey;

        if (!isDescending) {
            if (includeStart) {
                currentKey = new AtomicInteger(startKey);
            } else {
                currentKey = new AtomicInteger(startKey + 1);
            }
        } else {
            if (includeEnd) {
                currentKey = new AtomicInteger(endKey);
            } else {
                currentKey = new AtomicInteger(endKey - 1);
            }
        }
        Semaphore readLock = new Semaphore(0);
        Semaphore writeLock = new Semaphore(0);

        Thread scanThread = new Thread(() -> {

            String startKeyString = generateString(startKey, KEY_SIZE);
            String endKeyString = generateString(endKey, KEY_SIZE);

            try (OakMap<String, String> submap = oak.subMap(startKeyString, includeStart, endKeyString, includeEnd,
                    isDescending)) {

                Iterator<Map.Entry<String, String>> iterator = submap.entrySet().iterator();

                writeLock.release();
                int i = 0;
                while (iterator.hasNext()) {
                    try {
                        readLock.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String expectedKey = generateString(currentKey.get(), KEY_SIZE);
                    String expectedVal = generateString(currentKey.get(), VALUE_SIZE);
                    Map.Entry<String, String> entry = iterator.next();
                    Assert.assertEquals(expectedKey, entry.getKey());
                    Assert.assertEquals(expectedVal, entry.getValue());
                    writeLock.release();
                    if (!isDescending) {
                        currentKey.getAndIncrement();
                    } else {
                        currentKey.getAndDecrement();
                    }
                    i++;
                }

                int expectedIterations = endKey - startKey + 1;
                if (!includeEnd) {
                    expectedIterations--;
                }
                if (!includeStart) {
                    expectedIterations--;
                }

                Assert.assertEquals(expectedIterations, i);
                passed.set(true);
            }
            writeLock.release();
            continueWriting.set(false);

        });
        scanThread.start();


        Thread putThread = new Thread(() -> {

            // First block to prevent starting before reader thread init iterator
            try {
                writeLock.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (continueWriting.get()) {
                for (int j = 0; j < 200; j++) {
                    String key = String.format("%0$" + KEY_SIZE + "s", String.valueOf(currentKey));
                    String val = String.format("%0$" + VALUE_SIZE + "s", String.valueOf(currentKey));
                    oak.zc().remove(key);
                    oak.zc().put(key, val);
                }
                try {
                    readLock.release();
                    writeLock.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        putThread.start();
        scanThread.join();
        putThread.join();
        Assert.assertTrue(passed.get());
    }

    @Test
    public void concurrentModificationTest() throws InterruptedException {

        CountDownLatch deleteLatch = new CountDownLatch(1);
        CountDownLatch scanLatch = new CountDownLatch(1);
        AtomicBoolean passed = new AtomicBoolean(false);

        Thread scanThread = new Thread(() -> {
            Iterator<Map.Entry<String, String>> iterator = oak.entrySet().iterator();
            Assert.assertTrue(iterator.hasNext());
            deleteLatch.countDown();
            try {
                scanLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (iterator.next() == null) {
                    passed.set(true);
                }
            } catch (NoSuchElementException e) {
                passed.set(true);
            }
        });

        Thread deleteThread = new Thread(() -> {
            try {
                deleteLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < ELEMENTS; i++) {
                String key = String.format("%0$" + KEY_SIZE + "s", String.valueOf(i));
                oak.zc().remove(key);
            }
            scanLatch.countDown();
        });

        scanThread.start();
        deleteThread.start();

        scanThread.join();
        deleteThread.join();
        Assert.assertTrue(passed.get());
    }

    @Test
    public void concurrentModificationStreamTest() throws InterruptedException {

        CountDownLatch deleteLatch = new CountDownLatch(1);
        CountDownLatch scanLatch = new CountDownLatch(1);
        AtomicBoolean passed = new AtomicBoolean(false);

        Thread scanThread = new Thread(() -> {
            Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iterator = oak.zc().entryStreamSet().iterator();
            Assert.assertTrue(iterator.hasNext());
            deleteLatch.countDown();
            try {
                scanLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (iterator.next() == null) {
                    passed.set(true);
                }
            } catch (NoSuchElementException e) {
                passed.set(true);
            }
        });

        Thread deleteThread = new Thread(() -> {
            try {
                deleteLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < ELEMENTS; i++) {
                String key = String.format("%0$" + KEY_SIZE + "s", String.valueOf(i));
                oak.zc().remove(key);
            }
            scanLatch.countDown();
        });

        scanThread.start();
        deleteThread.start();

        scanThread.join();
        deleteThread.join();
        Assert.assertTrue(passed.get());
    }

    @Ignore
    @Test
    public void valueDeleteTest() {
        Iterator<Map.Entry<String, String>> entryIterator = oak.entrySet().iterator();
        Assert.assertTrue(entryIterator.hasNext());
        oak.zc().remove(generateString(0, KEY_SIZE));
        Assert.assertTrue(entryIterator.hasNext());
        Assert.assertNull(entryIterator.next());

        Iterator<String> valueIterator = oak.values().iterator();
        Assert.assertTrue(valueIterator.hasNext());
        oak.zc().remove(generateString(1, KEY_SIZE));
        Assert.assertTrue(valueIterator.hasNext());
        Assert.assertNull(valueIterator.next());

        Iterator<OakUnscopedBuffer> bufferValuesIterator = oak.zc().values().iterator();
        Assert.assertTrue(bufferValuesIterator.hasNext());
        oak.zc().remove(generateString(2, KEY_SIZE));
        Assert.assertTrue(bufferValuesIterator.hasNext());
        Assert.assertNull(bufferValuesIterator.next());

        Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> bufferEntriesIterator =
                oak.zc().entrySet().iterator();
        Assert.assertTrue(bufferEntriesIterator.hasNext());
        oak.zc().remove(generateString(3, KEY_SIZE));
        Assert.assertTrue(bufferEntriesIterator.hasNext());
        Assert.assertNull(bufferEntriesIterator.next());
    }
}
