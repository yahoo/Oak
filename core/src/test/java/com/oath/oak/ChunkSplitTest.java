package com.oath.oak;


import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentSkipListMap;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChunkSplitTest {
    private static final int maxItemsPerChunk = 10;

    @Test
    public void testSplitByCount() throws NoSuchFieldException, IllegalAccessException {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(1024)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");
        OakMap<String, String> oak = builder.build();

        for (int i = 0; i < maxItemsPerChunk + 1; i++) {
            String key = String.format("-%01d", i);
            oak.zc().put(key, key);
        }

        Field field = oak.getClass().getDeclaredField("internalOakMap");
        field.setAccessible(true);
        InternalOakMap<String, String> internalOakMap = (InternalOakMap<String, String>) field.get(oak);

        Field skipListField = internalOakMap.getClass().getDeclaredField("skiplist");
        skipListField.setAccessible(true);
        ConcurrentSkipListMap<Object, Chunk<String, String>> skipList = (ConcurrentSkipListMap<Object, Chunk<String, String>>) skipListField.get(internalOakMap);
        assertTrue(skipList.size() > 1);
        oak.close();
    }

    @Test
    public void testSplitByGet() {
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(1024);
        OakMap<Integer, Integer> oak = builder.build();
        Integer value;
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }

        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }

        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
            value = oak.get(0);
            assertEquals(0, value.intValue());
        }
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value.intValue());
        }


        for (Integer i = 1; i < (2 * maxItemsPerChunk - 1); i++) {
            oak.zc().remove(i);
        }

        value = oak.get(0);
        assertEquals(0, value.intValue());

        value = oak.get(2 * maxItemsPerChunk - 1);
        assertEquals(2 * maxItemsPerChunk - 1, value.intValue());
    }

    // the size of the keys is no longer the requirement for the split
}
