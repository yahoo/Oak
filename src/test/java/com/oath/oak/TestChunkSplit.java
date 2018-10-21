package com.oath.oak;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestChunkSplit {


    @Before
    public void init() {

    }

    @After
    public void finish() throws Exception{

    }

    @Test
    public void testSplitByCount() throws NoSuchFieldException, IllegalAccessException {
        OakMapBuilder builder = new OakMapBuilder()
                .setChunkMaxItems(10)
                .setChunkBytesPerItem(1024)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");
        OakMap<String, String> oak =  builder.build();

        for (int i=0; i < 11; i++) {
            String key = String.format("-%01d", i);
            oak.put(key,key);
        }

        Field field = oak.getClass().getDeclaredField("internalOakMap");
        field.setAccessible(true);
        InternalOakMap<String, String> internalOakMap = (InternalOakMap<String, String>) field.get(oak);

        Field skipListField = internalOakMap.getClass().getDeclaredField("skiplist");
        skipListField.setAccessible(true);
        ConcurrentSkipListMap<Object, Chunk<String, String>> skipList = (ConcurrentSkipListMap<Object, Chunk<String, String>>) skipListField.get(internalOakMap);
        assertTrue(skipList.size() > 1);
    }

    @Test
    public void testSplitBySize() throws NoSuchFieldException, IllegalAccessException {
        OakMapBuilder builder = new OakMapBuilder()
                .setChunkMaxItems(16)
                .setChunkBytesPerItem(1024)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");
        OakMap<String, String> oak =  builder.build();

        for (int i=0; i < 10; i++) {
            String key = String.format("-%01024d", i);
            oak.put(key,key);
        }

        Field field = oak.getClass().getDeclaredField("internalOakMap");
        field.setAccessible(true);
        InternalOakMap<String, String> internalOakMap = (InternalOakMap<String, String>) field.get(oak);

        Field skipListField = internalOakMap.getClass().getDeclaredField("skiplist");
        skipListField.setAccessible(true);
        ConcurrentSkipListMap<Object, Chunk<String, String>> skipList =
                (ConcurrentSkipListMap<Object, Chunk<String, String>>) skipListField.get(internalOakMap);
        assertTrue(skipList.size() > 1);
    }

    static final class StringSerializer implements OakSerializer<String> {

        @Override
        public void serialize(String object, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), object.length());
            for (int i = 0; i < object.length(); i++) {
                char c = object.charAt(i);
                targetBuffer.putChar(Integer.BYTES + targetBuffer.position() + i * Character.BYTES, c);
            }
        }

        @Override
        public String deserialize(ByteBuffer byteBuffer) {
            int size = byteBuffer.getInt(byteBuffer.position());
            StringBuilder object = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = byteBuffer.getChar(Integer.BYTES + byteBuffer.position() + i * Character.BYTES);
                object.append(c);
            }
            return object.toString();
        }

        @Override
        public int calculateSize(String object) {
            return Integer.BYTES + object.length() * Character.BYTES;
        }
    }


    public static class StringComparator implements OakComparator<String>{

        @Override
        public int compareKeys(String key1, String key2) {
            int result = key1.compareTo(key2);
            if (result > 0)
                return 1;
            else if (result < 0)
                return -1;
            else
                return 0;
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {

            int size1 = serializedKey1.getInt(serializedKey1.position());
            StringBuilder key1 = new StringBuilder(size1);
            for (int i = 0; i < size1; i++) {
                char c = serializedKey1.getChar(Integer.BYTES + serializedKey1.position() + i*Character.BYTES);
                key1.append(c);
            }

            int size2 = serializedKey2.getInt(serializedKey2.position());
            StringBuilder key2 = new StringBuilder(size2);
            for (int i = 0; i < size2; i++) {
                char c = serializedKey2.getChar(Integer.BYTES + serializedKey2.position() + i*Character.BYTES);
                key2.append(c);
            }

            return compareKeys(key1.toString(), key2.toString());
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, String key) {
            int size = serializedKey.getInt(serializedKey.position());
            StringBuilder key1 = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = serializedKey.getChar(Integer.BYTES + serializedKey.position() + i*Character.BYTES);
                key1.append(c);
            }
            return compareKeys(key1.toString(), key);
        }

    }



}
