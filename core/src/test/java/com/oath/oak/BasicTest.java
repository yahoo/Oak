package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BasicTest {

    private OakMap<String, String> oak;

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
    }

    @After
    public void tearDown() {
        oak.close();
    }

    @Test
    public void testPutGet(){
        for (int i = 0; i < 1000; ++i) {
            String key = String.valueOf(i);
            String value = String.valueOf(i);
            oak.put(key, value);
        }
        for (int i = 0; i < 1000; ++i) {
            String key = String.valueOf(i);
            String value = oak.get(key);
            assertEquals(key, value);
        }
    }





}
