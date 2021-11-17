/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalMap;

/**
 * Collects and report statistics on the memory usage of the map.
 */
class MemoryUsageStats {
    String title;
    float heapSize;
    float heapFreeSize;
    float heapUsed;
    float directUsed;
    float totalUsed;
    float totalAllocated;

    MemoryUsageStats(String title, CompositionalMap map) {
        System.gc();
        this.title = title;
        this.heapSize = (float) Runtime.getRuntime().totalMemory() / (float) Test.GB;
        this.heapFreeSize = (float) Runtime.getRuntime().freeMemory() / (float) Test.GB;
        this.heapUsed = heapSize - heapFreeSize;
        this.directUsed = map.nonHeapAllocatedGB();
        this.totalUsed = heapUsed + (Double.isNaN(directUsed) ? 0 : directUsed);
        this.totalAllocated = heapSize + (Double.isNaN(directUsed) ? 0 : directUsed);
    }

    static String HEADER_ROW = " %40s | %15s | %15s | %15s | %15s | %15s%n";
    static String DATA_ROW = " %40s | %12.4f GB | %12.4f GB | %12.4f GB | %12.4f GB | %12.4f GB%n";

    void printHeaderRow() {
        String header = String.format(HEADER_ROW,
            "", "Heap Size", "Heap Usage", "Off-Heap Usage", "Total Usage", "Total Allocated");
        System.out.print(header);
        System.out.println(PrintTools.dashLine(header.length()));
    }

    void printDataRow() {
        System.out.printf(DATA_ROW, title, heapSize, heapUsed, directUsed, totalUsed, totalAllocated);
    }
}
