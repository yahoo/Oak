package com.oath.oak;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class OverheadTest {
    private static final int K = 1024;
    private static final int M = K * K;
    private static final int NUM_OF_ENTRIES = 2_000_000;
    private static final int KEY_SIZE = 100;
    private static final int VALUE_SIZE = 1000;
    private static final double MAX_ON_HEAP_OVERHEAD_PERCENTAGE = 0.05;
    private static OakMap<Integer, Integer> oak;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = ToolsFactory.getDefaultIntBuilder()
                .setChunkMaxItems(100)
                .setKeySerializer(ToolsFactory.getOakIntSerializable(KEY_SIZE))
                .setValueSerializer(ToolsFactory.getOakIntSerializable(VALUE_SIZE));

        oak = builder.build();
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
        double usedOffHeapMemoryMB = (double) (oak.getMemoryManager().allocated()) / M;

        double heapOverhead = usedHeapMemoryMB / (usedHeapMemoryMB + usedOffHeapMemoryMB);
        System.out.println("Observed On Heap Overhead: " + heapOverhead);
        assert heapOverhead < MAX_ON_HEAP_OVERHEAD_PERCENTAGE;
    }
}
