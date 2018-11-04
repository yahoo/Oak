package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class MemoryReleaseTest {


    @Before
    public void init() {

    }

    @After
    public void finish() throws Exception{

    }

    @Test
    public void testOakClose() {

        OakMapBuilder builder = new OakMapBuilder()
                .setChunkMaxItems(1024)
                .setChunkBytesPerItem(4096)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");
        OakMap<String, String> oak =  builder.build();

        int i = 0;

        try {
            for (i = 0; i > -1; ++i) {
                String key = String.format("-%01024d", i);
                String val = String.format("-%01024d", i);
                oak.put(key, val);
            }
        } catch (OutOfMemoryError e) {

        }

        oak.close();
        oak = builder.build();

        try {

            for (int j = 0; j < i/2; ++j) {
                String key = String.format("-%01024d", j);
                String val = String.format("-%01024d", j);
                oak.put(key, val);
            }
        } catch (OutOfMemoryError e) {
            fail("Buffers not free after oak close");
        }
        oak.close();
    }
}
