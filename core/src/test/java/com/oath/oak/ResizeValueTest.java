package com.oath.oak;

import com.oath.oak.common.OakCommonBuildersFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

public class ResizeValueTest {
    private OakMap<String, String> oak;

    @Before
    public void initStuff() {
        OakMapBuilder<String, String> builder = OakCommonBuildersFactory.getDefaultStringBuilder()
            .setChunkMaxItems(100);

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
        String transformed = valBuffer.transform(OakCommonBuildersFactory.defaultStringSerializer::deserialize);
        assertEquals(value, transformed);
        oak.zc().put(key, stringBuilder.toString());
        valBuffer = oak.zc().get(key);
        transformed = valBuffer.transform(OakCommonBuildersFactory.defaultStringSerializer::deserialize);
        assertEquals(stringBuilder.toString(), transformed);
    }

    @Test
    public void retryIteratorTest() {
        oak.zc().put("AAAAAAA", "h");
        oak.zc().put("ZZZZZZZ", "h");

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(i);
        }
        String longValue = stringBuilder.toString();

        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);
        Thread iteratorThread = new Thread(() -> {
            Iterator<Map.Entry<String, String>> iterator = oak.entrySet().iterator();
            Iterator<String> valueIterator = oak.values().iterator();

            Map.Entry<String, String> entry = iterator.next();
            String currentValue = valueIterator.next();
            assertEquals("AAAAAAA", entry.getKey());
            assertEquals("h", currentValue);
            semaphore1.release();
            try {
                semaphore2.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            entry = iterator.next();
            currentValue = valueIterator.next();
            assertEquals("ZZZZZZZ", entry.getKey());
            assertEquals(longValue, entry.getValue());
            assertEquals(longValue, currentValue);
        });

        Thread modifyThread = new Thread(() -> {
            try {
                semaphore1.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            oak.zc().put("ZZZZZZZ", longValue);
            semaphore2.release();
        });
        iteratorThread.start();
        modifyThread.start();

        try {
            iteratorThread.join();
            modifyThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testResizeWithZCGet() {
        oak.zc().put("A", "");
        OakRBuffer buffer = oak.zc().get("A");
        assertEquals(0, buffer.getInt(0));
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(i);
        }
        String longValue = stringBuilder.toString();
        oak.zc().put("A", longValue);
        assertEquals(longValue.length(), buffer.getInt(0));
    }

    private static final int BLOCK_SIZE = 256 * 1024 * 1024;

    @Test
    public void testResizeWithZCGetNewBuffer() {
        String smallValue = "";
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.setLength((BLOCK_SIZE / Character.BYTES) / 2);
        String longValue = stringBuilder.toString();

        oak.zc().put("A", "");
        oak.zc().put("B", longValue);
        OakRBuffer buffer = oak.zc().get("A");
        assertEquals(0, buffer.getInt(0));

        oak.zc().put("A", longValue);
        assertEquals(longValue.length(), buffer.getInt(0));
    }
}
