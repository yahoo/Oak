/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HeapUsageTest {

    private static final long K = 1024;
    private static final long M = K * K;
    private static int keySize = 10;
    private static int valSize = (int) Math.round(5 * K);

    public static class FillTestKeySerializer implements OakSerializer<Integer> {

        @Override
        public void serialize(Integer key, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), key);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedKey) {
            return serializedKey.getInt(serializedKey.position());
        }

        @Override
        public int calculateSize(Integer key) {
            return keySize;
        }
    }

    public static class FillTestValueSerializer implements OakSerializer<Integer> {

        @Override
        public void serialize(Integer value, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), value);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.getInt(serializedValue.position());
        }

        @Override
        public int calculateSize(Integer value) {
            return valSize;
        }
    }

    @Test
    public void testMain() throws InterruptedException {

        OakMapBuilder<Integer, Integer> builder = OakMapBuilder
                .getDefaultBuilder()
                .setChunkMaxItems(2048)
                .setChunkBytesPerItem(100)
                .setKeySerializer(new FillTestKeySerializer())
                .setValueSerializer(new FillTestValueSerializer());
        // this number can be changed to test larger sizes however JVM memory limit need to be changed
        // otherwise this will hit "java.lang.OutOfMemoryError: Direct buffer memory" exception
        // currently tested up to 2GB
        int numOfEntries = 360000;

        //System.out.println("key size: " + keySize + "B" + ", value size: " + ((double) valSize) / K + "KB");

        Integer key = 0;
        Integer val = 0;


        try (OakMap<Integer, Integer> oak = builder.build()) {

//            long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
//            long heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
//            long heapFreeSize = Runtime.getRuntime().freeMemory();

//            System.out.println("\nBefore filling up oak");
//            System.out.println(
//                "heap size: " + heapSize / M + "MB" + ", heap max size: " + heapMaxSize / M + "MB" + ", heap free size: " + heapFreeSize / M + "MB");
//            System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");
//            System.out.println("off heap used: " + oak.getMemoryManager().allocated() / M + "MB");

            for (int i = 0; i < numOfEntries; i++) {
                oak.zc().put(i, i);
            }
//            System.out.println("\nAfter filling up oak");
//            System.out.println("off heap used: " + oak.getMemoryManager().allocated() / M + "MB");
//
//            System.gc();
//
//            heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
//            heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
//            heapFreeSize = Runtime.getRuntime().freeMemory();
//            System.out.println(
//                "heap size: " + heapSize / M + "MB" + ", heap max size: " + heapMaxSize / M + "MB" + ", heap free size: " + heapFreeSize / M + "MB");
//            System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");

            for (int i = 0; i < numOfEntries; i++) {
                oak.zc().put(i, i);
            }

            for (Integer i = 0; i < numOfEntries; i++) {
                Integer value = oak.get(i);
                assertEquals(i, value);
            }
//            System.out.println("\nCheck again");
//            System.out.println("off heap used: " + oak.getMemoryManager().allocated() / M + "MB");
//            System.out.println("off heap allocated: " + Integer.MAX_VALUE / M + "MB");
//            System.gc();
//            heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
//            heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
//            heapFreeSize = Runtime.getRuntime().freeMemory();
//            System.out.println(
//                "heap size: " + heapSize / M + "MB" + ", heap max size: " + heapMaxSize / M + "MB" + ", heap free size: " + heapFreeSize / M + "MB");
//            System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");
//            float percent = (100 * (heapSize - heapFreeSize)) / oak.getMemoryManager().allocated();
//            System.out.println("\non/off heap used: " + String.format("%.0f%%", percent));
        }
    }

    @Ignore
    @Test
    public void testUsage() {
        // this number can be changed to test larger sizes however JVM memory limit need to be changed
        // otherwise this will hit "java.lang.OutOfMemoryError: Direct buffer memory" exception
        // currently tested up to 2GB
        List<Long> configurations = new ArrayList<>();
        for (int i = 50; i < 300; i += 50) {
            configurations.add(i * K);
        }

        System.out.println("key size: " + keySize + "B" + ", value size: " + ((double) valSize) / K + "KB");
        for (long numOfEntries : configurations) {
            OakMapBuilder<Integer, Integer> builder = OakMapBuilder
                    .getDefaultBuilder()
                    .setChunkMaxItems(2048)
                    .setChunkBytesPerItem(100)
                    .setKeySerializer(new FillTestKeySerializer())
                    .setValueSerializer(new FillTestValueSerializer());
            try (OakMap<Integer, Integer> oak = builder.build()) {
                System.out.println("=====================================\nWith " + numOfEntries + " entries");
                long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
                long heapFreeSize = Runtime.getRuntime().freeMemory();

                System.out.println("\nBefore filling up oak");
                System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");
                System.out.println("off heap used: " + oak.getMemoryManager().allocated() / M + "MB");

                for (int i = 0; i < numOfEntries; i++) {
                    oak.zc().put(i, i);
                }
                System.out.println("\nAfter filling up oak");
                System.gc();

                heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
                heapFreeSize = Runtime.getRuntime().freeMemory();
                System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");
                System.out.println("off heap used: " + oak.getMemoryManager().allocated() / M + "MB");

                double percent = (100.0 * (heapSize - heapFreeSize)) / (oak.getMemoryManager().allocated() * 1.0);
                System.out.println("\non/off heap used: " + String.format("%.2f%%", percent));
                oak.getMemoryManager().close();
            }
        }
        assert true;
    }

}
