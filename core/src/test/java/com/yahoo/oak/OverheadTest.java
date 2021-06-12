/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class OverheadTest {
    private static final int K = 1024;
    private static final int M = K * K;
    private static final int NUM_OF_ENTRIES = 2_000_000;
    private static final int KEY_SIZE = 100;
    private static final int VALUE_SIZE = 1000;
    private static final double MAX_ON_HEAP_OVERHEAD_PERCENTAGE = 0.05;
    private static ConcurrentZCMap<Integer, Integer> oak;

    private final Supplier<ConcurrentZCMap<Integer, Integer>> supplier;


    public OverheadTest(Supplier<ConcurrentZCMap<Integer, Integer>> supplier) {
        this.supplier = supplier;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {

        Supplier<ConcurrentZCMap<Integer, Integer>> s1 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setChunkMaxItems(100)
                    .setKeySerializer(new OakIntSerializer(KEY_SIZE))
                    .setValueSerializer(new OakIntSerializer(VALUE_SIZE));

            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap<Integer, Integer>> s2 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setChunkMaxItems(100)
                    .setKeySerializer(new OakIntSerializer(KEY_SIZE))
                    .setValueSerializer(new OakIntSerializer(VALUE_SIZE));

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

    @Test
    public void main() {
        Random r = new Random();
        for (int i = 0; i < (int) Math.round(NUM_OF_ENTRIES * 0.5); ) {
            Integer key = r.nextInt(NUM_OF_ENTRIES);
            if (oak.putIfAbsent(key, 8) == null) {
                i++;
            }
        }

        System.gc();
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        double usedHeapMemoryMB = (double) (heapSize - heapFreeSize) / M;
        double usedOffHeapMemoryMB = (double) (oak.getValuesMemoryManager().allocated()) / M;

        double heapOverhead = usedHeapMemoryMB / (usedHeapMemoryMB + usedOffHeapMemoryMB);
        System.out.println("Observed On Heap Overhead: " + heapOverhead);
        assert heapOverhead < MAX_ON_HEAP_OVERHEAD_PERCENTAGE;
    }
}
