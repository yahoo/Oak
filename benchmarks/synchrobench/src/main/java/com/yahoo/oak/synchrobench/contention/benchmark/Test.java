/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalMap;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;

import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Synchrobench-java, a benchmark to evaluate the implementations of
 * high level abstractions including Map and Set.
 *
 * @author Vincent Gramoli
 */
public class Test {
    public static final double MEBI_OPS = 1L << 20;
    public static final double GB = 1L << 30;

    // The array of threads executing the benchmark
    private Thread[] threads;

    // The array of runnable thread codes
    private ThreadLoop[] threadLoops = new ThreadLoop[0];

    // Collected iteration stats
    private final OpCounter.Stats[] stats;

    /**
     * The instance of the benchmark
     */
    private final CompositionalMap oakBench;
    private final KeyGenerator keyGen;
    private final ValueGenerator valueGen;
    private BenchKey lastKey = null;

    /**
     * The thread-private PRNG
     */
    private static final ThreadLocal<Random> S_RANDOM = new ThreadLocal<Random>() {
        @Override
        protected synchronized Random initialValue() {
            return new Random();
        }
    };

    /**
     * Constructor sets up the benchmark by reading parameters and creating
     * threads
     *
     * @param args the arguments of the command-line
     */
    public Test(String[] args) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        printSynchrobenchHeader();
        Parameters.parseCommandLineParameters(args);
        Parameters.print();
        System.out.println();

        this.stats = new OpCounter.Stats[Parameters.confIterations];

        // Instantiate abstraction
        Class<?> keyClass = Class.forName(String.format("%s.KeyGen", Parameters.confKeyClass));
        keyGen = (KeyGenerator) keyClass.getConstructor(Integer.class).newInstance(Parameters.confKeySize);

        Class<?> valueClass = Class.forName(String.format("%s.ValueGen", Parameters.confValueClass));
        valueGen = (ValueGenerator) valueClass.getConstructor(Integer.class).newInstance(Parameters.confValSize);

        Class<?> benchClass = Class.forName(Parameters.confBenchClass);
        if (!CompositionalMap.class.isAssignableFrom(benchClass)) {
            throw new IllegalArgumentException("Only 'CompositionalOakMap' is supported.");
        }

        oakBench = (CompositionalMap) benchClass.getConstructor(
            KeyGenerator.class, ValueGenerator.class).newInstance(keyGen, valueGen);
    }

    public void fill(final int range, final long size) {
        long operations = 0;
        final Random valueRand = S_RANDOM.get();
        final Random keyRand = (Parameters.confKeyDistribution == Parameters.KeyDist.RANDOM) ? valueRand : null;
        final long reportGran = size / 100;

        System.out.print("Filling data");
        final long startTime = System.currentTimeMillis();
        lastKey = null;
        for (long i = 0; i < size; ) {
            BenchKey curKey = keyGen.getNextKey(keyRand, range, lastKey);
            BenchValue curValue = valueGen.getNextValue(valueRand, range);

            if (oakBench.putIfAbsentOak(curKey, curValue)) {
                i++;
            }
            // counts all the putIfAbsent operations, not only the successful ones
            operations++;

            lastKey = curKey;

            if (reportGran == 0 || (i % reportGran) == 0) {
                System.out.printf("\rFilling data: %.0f%%", (float) i * 100 / (float) size);
            }
        }
        final long endTime = System.currentTimeMillis();
        double initTime = ((double) (endTime - startTime)) / 1000.0;
        System.out.printf("\rInitialization complete in %,.4f (seconds) - %,d operations%n", initTime, operations);
    }


    /**
     * Creates as many threads as requested
     */
    private void initThreads() {
        threadLoops = new ThreadLoop[Parameters.confNumThreads];
        threads = new Thread[Parameters.confNumThreads];
        for (short threadNum = 0; threadNum < Parameters.confNumThreads; threadNum++) {
            threadLoops[threadNum] = new ThreadLoop(threadNum, oakBench, keyGen, valueGen, lastKey);
            threads[threadNum] = new Thread(threadLoops[threadNum]);
        }
    }

    class HeapStats {
        // Get current size of heap in bytes
        String title;
        float heapSize;
        float heapFreeSize;
        float heapUsed;
        float directUsed;
        float totalUsed;
        float totalAllocated;

        HeapStats(String title) {
            System.gc();
            this.title = title;
            this.heapSize = (float) Runtime.getRuntime().totalMemory() / (float) GB;
            this.heapFreeSize = (float) Runtime.getRuntime().freeMemory() /  (float) GB;
            this.heapUsed = heapSize - heapFreeSize;
            this.directUsed = oakBench.allocatedGB();
            this.totalUsed = heapUsed + directUsed;
            this.totalAllocated = heapSize + directUsed;
        }

        String row = " %40s | %9s | %10s | %14s | %11s | %15s%n";
        String dataRow = " %40s | %6.4f GB | %7.4f GB | %11.4f GB | %8.4f GB | %12.4f GB%n";

        void printHeaderRow() {
            String header = String.format(row,
                "", "Heap Size", "Heap Usage", "Off-Heap Usage", "Total Usage", "Total Allocated");
            System.out.print(header);
            System.out.println(PrintTools.dashLine(header.length()));
        }

        void printDataRow() {
            System.out.printf(dataRow, title, heapSize, heapUsed, directUsed, totalUsed, totalAllocated);
        }
    }

    /**
     * Execute the main thread that starts and terminates the benchmark threads
     */
    private double execute(int milliseconds, boolean isWarmup) throws Exception {
        reset();

        HeapStats s1 = null;
        if (!isWarmup) {
            s1 = new HeapStats("Before initial fill");
        }
        fill(Parameters.confRange, Parameters.confSize);
        if (!isWarmup) {
            s1.printHeaderRow();
            s1.printDataRow();
            new HeapStats("After initial fill, before benchmark").printDataRow();
        }

//        Thread.sleep(5000);
        initThreads();
        final long startTime = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        try {
            Thread.sleep(milliseconds);
        } finally {
            for (ThreadLoop threadLoop : threadLoops) {
                threadLoop.stopThread();
            }
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (ThreadLoop threadLoop : threadLoops) {
            if (threadLoop.error != null) {
                throw threadLoop.error;
            }
        }

        final long endTime = System.currentTimeMillis();
        double elapsedTime = ((double) (endTime - startTime)) / 1000.0;

        if (!isWarmup) {
            new HeapStats("After benchmark").printDataRow();
            System.out.println();
        }

        return elapsedTime;
    }

    public void iteration(int iteration) throws Exception {
        // Warmup iteration does not print statistics
        final boolean isWarmup = iteration < 0;

        if (isWarmup) {
            PrintTools.printHeader("Benchmark warmup");
        } else {
            PrintTools.printHeader("Benchmark iteration: %,d", iteration);
        }

        final int executeTime = isWarmup ? Parameters.confWarmupMilliseconds : Parameters.confNumMilliseconds;
        double elapsedTime = execute(executeTime, isWarmup);

        if (isWarmup) {
            System.out.println("Warmup complete");
        }

        if (!isWarmup) {
            OpCounter.Stats s = collectIterationStats(iteration, elapsedTime);
            s.printStats();

            if (Parameters.confDetailedStats) {
                oakBench.printMemStats();
            }
        }
    }

    public void run() throws Exception {
        if (Parameters.confWarmupMilliseconds != 0) {
            iteration(-1);
        }

        for (int i = 0; i < Parameters.confIterations; i++) {
            iteration(i);
        }

        printAggregatedIterationStats();
    }

    private OpCounter.Stats collectIterationStats(int iteration, double time) {
        stats[iteration] = new OpCounter.Stats(
            Stream.of(threadLoops).map(t -> t.counter).toArray(OpCounter[]::new),
            time, threadLoops.length, oakBench.size()
        );
        return stats[iteration];
    }

    /**
     * This method is called before each run of the benchmark.
     */
    public void reset() {
        Stream.of(threadLoops).forEach(ThreadLoop::reset);
        oakBench.clear();
    }

    /* ---------------- Input/Output -------------- */

    /**
     * Print Synchrobench header message on the standard output
     */
    private void printSynchrobenchHeader() {
        PrintTools.printHeader("Synchrobench-java: "
            + "A benchmark-suite to evaluate synchronization techniques");
    }

    /**
     * Print the aggregated iteration statistics on the standard output
     */
    private void printAggregatedIterationStats() {
        PrintTools.printHeader("Aggregated iteration statistics");

        final int n = Parameters.confIterations;
        System.out.println("  Iterations:                 \t" + n);
        double sum = 0;
        int sizeSum = 0;
        for (int i = 0; i < n; i++) {
            sum += stats[i].avgThroughput() / MEBI_OPS;
            sizeSum += stats[i].totalSize;
        }
        System.out.println("  Mean Total Size:              " + (double) sizeSum / n);

        System.out.println("  Throughput (mebiops/s):");
        double mean = sum / n;
        System.out.println("  |--Mean:                    \t" + mean);
        double temp = 0;
        for (int i = 0; i < n; i++) {
            double diff = (stats[i].avgThroughput() / MEBI_OPS) - mean;
            temp += diff * diff;
        }
        double var = temp / n;
        System.out.println("  |--Variance:                \t" + var);
        double stdEvp = java.lang.Math.sqrt(var);
        System.out.println("  |--Standard deviation pop:  \t" + stdEvp);
        double stErr = stdEvp / java.lang.Math.sqrt(n);
        System.out.println("  |--Standard error:          \t" + stErr);
        System.out.println("  |--Margin of error (95% CL):\t" + (stErr * 1.96));
        System.out.println();

        if (Parameters.confMeasureLatency) {
            OpCounter.Stats s = new OpCounter.Stats(Stream.of(stats).toArray(OpCounter.Stats[]::new));
            s.printStats();
        }
    }

    public static void main(String[] args) throws Exception {
        new Test(args).run();
    }
}
