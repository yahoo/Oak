package com.oath.oak;


import org.junit.Test;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentSkipListMap;
import static org.junit.Assert.assertTrue;

public class ChunkSplitTest {

    @Test
    public void testSplitByCount() throws NoSuchFieldException, IllegalAccessException {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(10)
                .setChunkBytesPerItem(1024)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");
        OakMap<String, String> oak =  builder.build();

        for (int i=0; i < 11; i++) {
            String key = String.format("-%01d", i);
            oak.zc().put(key,key);
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
    public void testSplitBySize() throws NoSuchFieldException, IllegalAccessException {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(16)
                .setChunkBytesPerItem(1024)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");
        OakMap<String, String> oak =  builder.build();

        for (int i=0; i < 10; i++) {
            String key = String.format("-%01024d", i);
            oak.zc().put(key,key);
        }

        Field field = oak.getClass().getDeclaredField("internalOakMap");
        field.setAccessible(true);
        InternalOakMap<String, String> internalOakMap = (InternalOakMap<String, String>) field.get(oak);

        Field skipListField = internalOakMap.getClass().getDeclaredField("skiplist");
        skipListField.setAccessible(true);
        ConcurrentSkipListMap<Object, Chunk<String, String>> skipList =
                (ConcurrentSkipListMap<Object, Chunk<String, String>>) skipListField.get(internalOakMap);
        assertTrue(skipList.size() > 1);
        oak.close();
    }
}
