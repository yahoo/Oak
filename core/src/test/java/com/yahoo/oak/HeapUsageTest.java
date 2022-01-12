/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class HeapUsageTest {

    private static final long K = 1024;
    private static final long M = K * K;
    private static int keySize = 10;
    private static int valSize = (int) Math.round(5 * K);

    private ConcurrentZCMap<Integer, Integer> oak;
    private Supplier<ConcurrentZCMap<Integer , Integer>> supplier;

    public HeapUsageTest(Supplier<ConcurrentZCMap<Integer , Integer>> supplier) {
        this.supplier = supplier;
    }

    @Parameterized.Parameters
    public static Collection parameters() {

        Supplier<ConcurrentZCMap<Integer , Integer>> s1 = () -> {
            int maxItemsPerChunk = 2048;
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setKeySerializer(new OakIntSerializer(keySize))
                .setValueSerializer(new OakIntSerializer(valSize));

            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap<Integer , Integer>> s2 = () -> {
            int maxItemsPerChunk = 512;
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setKeySerializer(new OakIntSerializer(keySize))
                .setValueSerializer(new OakIntSerializer(valSize));
            return builder.buildHashMap();
        };
        return Arrays.asList(new Object[][] {
            { s1 },
            { s2 }
        });
    }



    @Before
    public void init() {
        oak = supplier.get();
    }

    @After
    public void finish() {
        oak.close();
        BlocksPool.clear();
    }

    @Test
    public void testMain() throws InterruptedException {

        if (oak instanceof OakHashMap) {
            System.out.println("====== Results for OakHash ======");
        } else {
            System.out.println("====== Results for OakMap ======");
        }
        // this number can be changed to test larger sizes however JVM memory limit need to be changed
        // otherwise this will hit "java.lang.OutOfMemoryError: Direct buffer memory" exception
        // currently tested up to 2GB
        int numOfEntries = 300000;

        //System.out.println("key size: " + keySize + "B" + ", value size: " + ((double) valSize) / K + "KB");

        Integer key = 0;
        Integer val = 0;

        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

//        System.err.println("\nBefore filling up oak"); //TODO: to remove after memory usage is tuned
//        System.err.println(
//            "current heap allocated size: " + heapSize / M + "MB" + ", max heap size: " + heapMaxSize / M + "MB"
//                + ", free heap size: " + heapFreeSize / M + "MB");
//        System.err.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");
//        System.err.println("off heap used: " + oak.memorySize() / M + "MB");

        for (int i = 0; i < numOfEntries; i++) {
            oak.zc().put(i, i);
        }
//        System.out.println("\nAfter filling up oak"); //TODO: to remove after memory usage is tuned
//        System.out.println("off heap used: " + oak.memorySize() / M + "MB");

        System.gc();

        heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
        heapFreeSize = Runtime.getRuntime().freeMemory();
//        System.out.println( //TODO: to remove after memory usage is tuned
//            "current heap allocated size: " + heapSize / M + "MB" + ", max heap size: " + heapMaxSize / M + "MB"
//                + ", free heap size: " + heapFreeSize / M + "MB");
//        System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");

        for (int i = 0; i < numOfEntries; i++) {
            oak.zc().put(i, i);
        }

        for (Integer i = 0; i < numOfEntries; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }
//        System.out.println("\nCheck again"); //TODO: to remove after memory usage is tuned
//        System.out.println("off heap used: " + oak.memorySize() / M + "MB");
//        System.out.println("off heap allocated: " + Integer.MAX_VALUE / M + "MB");
        System.gc();
        heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
        heapFreeSize = Runtime.getRuntime().freeMemory();
//        System.out.println( //TODO: to remove after memory usage is tuned
//            "heap size: " + heapSize / M + "MB" + ", heap max size: " + heapMaxSize / M + "MB"
//                + ", heap free size: " + heapFreeSize / M + "MB");
        System.out.println("on-heap used: " + (heapSize - heapFreeSize) / M + "MB, off-heap used: "
            + oak.memorySize() / M + "MB");
        float percent = (100 * (heapSize - heapFreeSize)) / oak.memorySize();
        System.out.println("on/off heap used: " + String.format("%.0f%%", percent));

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
            try (OakMap<Integer, Integer> oak = builder.buildOrderedMap()) {
                System.out.println("=====================================\nWith " + numOfEntries + " entries");
                long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
                long heapFreeSize = Runtime.getRuntime().freeMemory();

                System.out.println("\nBefore filling up oak");
                System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");
                System.out.println("off heap used: " + oak.getValuesMemoryManager().allocated() / M + "MB");

                for (int i = 0; i < numOfEntries; i++) {
                    oak.zc().put(i, i);
                }
                System.out.println("\nAfter filling up oak");
                System.gc();

                heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
                heapFreeSize = Runtime.getRuntime().freeMemory();
                System.out.println("heap used: " + (heapSize - heapFreeSize) / M + "MB");
                System.out.println("off heap used: " + oak.getValuesMemoryManager().allocated() / M + "MB");

                double percent = (100.0 * (heapSize - heapFreeSize)) / (oak.getValuesMemoryManager().allocated() * 1.0);
                System.out.println("\non/off heap used: " + String.format("%.2f%%", percent));
                try {
                    oak.getValuesMemoryManager().close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        assert true;
    }

}
