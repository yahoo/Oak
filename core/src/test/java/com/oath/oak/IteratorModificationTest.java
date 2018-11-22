package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IteratorModificationTest {

    OakMap<String, String> oak;
    private static final int ELEMENTS = 1000;
    private static final int KEY_SIZE = 64;
    private static final int VALUE_SIZE= 64;

    @Before
    public void init() {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(100)
                .setChunkBytesPerItem(128)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");

        oak =  builder.build();

        for (int i = 0; i < ELEMENTS; i++) {
            String key = String.format("%0$" + KEY_SIZE + "s", String.valueOf(i));
            String val = String.format("%0$" + VALUE_SIZE + "s", String.valueOf(i));
            assert(key.length() == KEY_SIZE);
            assert(val.length() == VALUE_SIZE);
            oak.put(key, val);
        }
    }

    @After
    public void tearDown() {
        oak.close();
    }


    @Test
    public void continueIterationDuringRebalance() throws InterruptedException {

        AtomicBoolean passed = new AtomicBoolean(false);
        AtomicInteger i = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        Object readLock = new Object();
        Object writeLock = new Object();



        Thread scanThread = new Thread(() -> {

            OakCloseableIterator<Map.Entry<String, String>> iterator = oak.entriesIterator();
            latch.countDown();
            while (iterator.hasNext()) {

                try {
                    synchronized (readLock) {
                        readLock.wait();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                String expectedKey = String.format("%0$" + KEY_SIZE + "s", String.valueOf(i.get()));
                String expectedVal = String.format("%0$" + VALUE_SIZE + "s", String.valueOf(i.get()));

                Map.Entry<String, String> entry = iterator.next();
                assertEquals(expectedKey, entry.getKey());
                assertEquals(expectedVal, entry.getValue());
                i.getAndIncrement();
                synchronized (writeLock) {
                    writeLock.notify();
                }
            }
            assertEquals(ELEMENTS, i.get());
            passed.set(true);
        });
        scanThread.start();


        Thread putThread = new Thread(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (i.get() < ELEMENTS) {
                for (int j = 0; j < 200; j++) {
                    String key = String.format("%0$" + KEY_SIZE + "s", String.valueOf(i));
                    String val = String.format("%0$" + VALUE_SIZE + "s", String.valueOf(i));
                    oak.remove(key);
                    oak.put(key, val);
                }
                try {
                    synchronized (readLock) {
                        readLock.notify();
                    }
                    synchronized (writeLock) {
                        writeLock.wait();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        putThread.start();
        scanThread.join();
        putThread.join();
        assertTrue(passed.get());
    }

    @Test
    public void concurrentModificationTest(){
        //TODO
    }



}
