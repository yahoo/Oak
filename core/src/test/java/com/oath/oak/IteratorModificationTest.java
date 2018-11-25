package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
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


    @Test(timeout = 10000)
    public void descendingIterationDuringRebalanceExclude() throws InterruptedException {
        doIterationTest(99,
                false,
                ELEMENTS-99,
                false,
                true);
    }

    @Test(timeout = 10000)
    public void descendingIterationDuringRebalance() throws InterruptedException {
        doIterationTest(0,
                true,
                ELEMENTS-1,
                true,
                true);
    }

    @Test(timeout = 10000)
    public void iterationDuringRebalance() throws InterruptedException {
        doIterationTest(0,
                true,
                ELEMENTS-1,
                true,
                false);
    }

    @Test(timeout = 10000)
    public void iterationDuringRebalanceExclude() throws InterruptedException {
        doIterationTest(234,
                false,
                ELEMENTS-342,
                false,
                false);
    }


    public void doIterationTest(int startKey, boolean includeStart, int endKey, boolean includeEnd,
                                boolean  isDescending) throws InterruptedException {

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

            String startKeyString = String.format("%0$" + KEY_SIZE + "s", String.valueOf(startKey));
            String endKeyString = String.format("%0$" + VALUE_SIZE + "s", String.valueOf(endKey));

            try (OakMap<String, String> submap = oak.subMap(startKeyString, includeStart, endKeyString, includeEnd,isDescending)) {

                OakIterator<Map.Entry<String, String>> iterator = submap.entriesIterator();

                writeLock.release();
                int i = 0;
                while (iterator.hasNext()) {
                    try {
                        readLock.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String expectedKey = String.format("%0$" + KEY_SIZE + "s", String.valueOf(currentKey.get()));
                    String expectedVal = String.format("%0$" + VALUE_SIZE + "s", String.valueOf(currentKey.get()));
                    Map.Entry<String, String> entry = iterator.next();
                    assertEquals(expectedKey, entry.getKey());
                    assertEquals(expectedVal, entry.getValue());
                    writeLock.release();
                    if (!isDescending)
                        currentKey.getAndIncrement();
                    else
                        currentKey.getAndDecrement();
                    i++;
                }

                int expectedIterations = endKey - startKey + 1;
                if (!includeEnd) expectedIterations --;
                if (!includeStart) expectedIterations --;

                assertEquals(expectedIterations, i);
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
                    oak.remove(key);
                    oak.put(key, val);
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
        assertTrue(passed.get());
    }

//    @Test
//    public void concurrentModificationTest(){
//        //TODO
//    }



}
