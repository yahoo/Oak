package com.oath.oak;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

public class ResizeValueTest {
    private OakMap<String, String> oak;

    @Before
    public void initStuff() {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(100)
                .setChunkBytesPerItem(128)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");

        oak = builder.build();
    }

    @Test
    public void simpleSequentialResizeTest() {
        String key = "Hello";
        String value = "h";
        oak.zc().put(key, value);
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(i);
        }
        OakRBuffer valBuffer = oak.zc().get(key);
        String transformed = valBuffer.transform(b -> new StringSerializer().deserialize(b));
        assertEquals(value, transformed);
        oak.zc().put(key, stringBuilder.toString());
        valBuffer = oak.zc().get(key);
        transformed = valBuffer.transform(b -> new StringSerializer().deserialize(b));
        assertEquals(stringBuilder.toString(), transformed);
    }

    @Test
    public void retryIteratorTest() {
        oak.zc().put("AAAAAAA", "h");
        oak.zc().put("ZZZZZZZ", "h");

        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);
        Thread iteratorThread = new Thread(() -> {
            Iterator<Map.Entry<String, String>> iterator = oak.entrySet().iterator();

            Map.Entry<String, String> entry = iterator.next();
            assertEquals(entry.getKey(), "AAAAAAA");

            semaphore1.release();
            try {
                semaphore2.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });
        iteratorThread.start();
    }
}
