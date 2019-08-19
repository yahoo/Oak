package com.oath.oak;

import org.junit.Before;
import org.junit.Test;

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
    public void testMain() {
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
}
