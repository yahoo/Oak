/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HeapUsageTest {

    private static final long K = 1024;
    private static final long M = K * K;
    private static int keySize = 10;
    private static int valSize = (int) Math.round(5 * K);

    @Test
    public void testMain() throws InterruptedException {

        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(2048)
                .setKeySerializer(new OakIntSerializer(keySize))
                .setValueSerializer(new OakIntSerializer(valSize));
        // this number can be changed to test larger sizes however JVM memory limit need to be changed
        // otherwise this will hit "java.lang.OutOfMemoryError: Direct buffer memory" exception
        // currently tested up to 2GB
        int numOfEntries = 160000;

        //System.out.println("key size: " + keySize + "B" + ", value size: " + ((double) valSize) / K + "KB");

        Integer key = 0;
        Integer val = 0;


        try (OakMap<Integer, Integer> oak = builder.build()) {

//            long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
//            long heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
//            long heapFreeSize = Runtime.getRuntime().freeMemory();

//            System.out.println("\nBefore filling up oak");
//            System.out.println(
//                "heap size: " + heapSize / M + "MB" + ", heap max size: " + heapMaxSize / M + "MB" + ", heap free
//                size: " + heapFreeSize / M + "MB");
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
//                "heap size: " + heapSize / M + "MB" + ", heap max size: " + heapMaxSize / M + "MB" + ", heap free
//                size: " + heapFreeSize / M + "MB");
//            System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");

            for (int i = 0; i < numOfEntries; i++) {
                oak.zc().put(i, i);
            }

            for (Integer i = 0; i < numOfEntries; i++) {
                Integer value = oak.get(i);
                Assert.assertEquals(i, value);
            }
//            System.out.println("\nCheck again");
//            System.out.println("off heap used: " + oak.getMemoryManager().allocated() / M + "MB");
//            System.out.println("off heap allocated: " + Integer.MAX_VALUE / M + "MB");
//            System.gc();
//            heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
//            heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
//            heapFreeSize = Runtime.getRuntime().freeMemory();
//            System.out.println(
//                "heap size: " + heapSize / M + "MB" + ", heap max size: " + heapMaxSize / M + "MB" + ", heap free
//                size: " + heapFreeSize / M + "MB");
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
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setChunkMaxItems(2048)
                    .setKeySerializer(new OakIntSerializer(keySize))
                    .setValueSerializer(new OakIntSerializer(valSize));
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
                try {
                    oak.getMemoryManager().close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        assert true;
    }

}
