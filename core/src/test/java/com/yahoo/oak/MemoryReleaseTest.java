/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.Test;


public class MemoryReleaseTest {

    @Test(timeout = 300_000)
    public void testByteBuffersReleased() {
//        System.gc();
//        String val = String.format("-%016000d", 0);
//
//        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
//                .setChunkMaxItems(1024)
//                .setChunkBytesPerItem(4096)
//                .setKeySerializer(new StringSerializer())
//                .setValueSerializer(new StringSerializer())
//                .setComparator(new StringComparator())
//                .setMinKey("");
//        OakMap<String, String> oak =  builder.build();
//
//        int firstIteration = 0;
//        try {
//            while (true) {
//                String key = String.format("-%01024d", firstIteration++);
//                oak.put(key, val);
//            }
//        } catch (OutOfMemoryError e) {
//
//        }
//
//        oak.close();
//
//        int secondIteration = 0;
//        oak =  builder.build();
//        System.gc();
//
//        try {
//            while (true) {
//                String key = String.format("-%01024d", secondIteration++);
//                oak.put(key, val);
//            }
//        } catch (OutOfMemoryError e) {
//
//        }
//        assert(firstIteration <= secondIteration);
//        oak.close();
//        System.gc();
    }

}
