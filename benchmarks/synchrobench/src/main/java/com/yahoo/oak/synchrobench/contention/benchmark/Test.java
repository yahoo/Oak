/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalMap;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;

import java.lang.reflect.InvocationTargetException;
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

    // Collected iteration stats
    private final OpCounter.Stats[] stats;

    // The instance of the benchmark
    private final CompositionalMap oakBench;
    private final KeyGenerator keyGen;
    private final ValueGenerator valueGen;

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

    public void fill(final long size) throws InterruptedException {
        // Non-random key distribution can only be initialized from one thread.
        final int numWorkers = Parameters.isRandomKeyDistribution() ? Parameters.confNumFillThreads : 1;
        FillWorker[] fillWorkers = new FillWorker[numWorkers];
        Thread[] fillThreads = new Thread[numWorkers];
        final long sizePerThread = size / numWorkers;
        final long reminder = size % numWorkers;
        for (int i = 0; i < numWorkers; i++) {
            final long sz = i < reminder ? sizePerThread + 1 : sizePerThread;
            fillWorkers[i] = new FillWorker(oakBench, keyGen, valueGen, sz);
            fillThreads[i] = new Thread(fillWorkers[i]);
        }
        final long reportGranMS = 200;
        final boolean isConsole = System.console() != null;

        System.out.print("Start filling data...");
        if (!isConsole) {
            System.out.println();
        }

        final long startTime = System.currentTimeMillis();
        for (Thread thread : fillThreads) {
            thread.start();
        }

        try {
            if (isConsole) {
                while ((oakBench.size() < size) && Stream.of(fillThreads).anyMatch(Thread::isAlive)) {
                    long operations = Stream.of(fillWorkers).mapToLong(FillWorker::getOperations).sum();
                    final long curTime = System.currentTimeMillis();
                    double runTime = ((double) (curTime - startTime)) / 1000.0;
                    System.out.printf(
                        "\rFilling data: %5.0f%% -- %,6.2f (seconds) - %,d operations",
                        (float) oakBench.size() * 100 / (float) size,
                        runTime,
                        operations
                    );
                    Thread.sleep(reportGranMS);
                }
            }
        } catch (InterruptedException e) {
            System.out.println("\nFilling was interrupted. Waiting to finish.");
        } finally {
            for (Thread t : fillThreads) {
                t.join();
            }
        }
        final long endTime = System.currentTimeMillis();
        double initTime = ((double) (endTime - startTime)) / 1000.0;
        long operations = Stream.of(fillWorkers).mapToLong(FillWorker::getOperations).sum();

        if (isConsole) {
            System.out.print("\r");
        }
        System.out.printf("Initialization complete in %,.4f (seconds) - %,d operations%n", initTime, operations);
    }

    private OpCounter.Stats collectIterationStats(BenchLoopWorker[] workers, double time) {
        return new OpCounter.Stats(
            Stream.of(workers).map(t -> t.counter).toArray(OpCounter[]::new),
            time, workers.length, oakBench.size()
        );
    }

    /**
     * Execute the main thread that starts and terminates the benchmark threads
     */
    private OpCounter.Stats execute(int milliseconds, boolean isWarmup) throws Exception {
        MemoryUsageStats s1 = null;
        if (!isWarmup) {
            s1 = new MemoryUsageStats("Before initial fill", oakBench);
        }
        fill(Parameters.confSize);
        if (!isWarmup) {
            s1.printHeaderRow();
            s1.printDataRow();
            new MemoryUsageStats("After initial fill, before benchmark", oakBench).printDataRow();
        }

        BenchLoopWorker[] benchLoopWorkers = new BenchLoopWorker[Parameters.confNumThreads];
        Thread[] threads = new Thread[Parameters.confNumThreads];
        for (int i = 0; i < Parameters.confNumThreads; i++) {
            benchLoopWorkers[i] = new BenchLoopWorker(oakBench, keyGen, valueGen);
            threads[i] = new Thread(benchLoopWorkers[i]);
        }

        final long startTime = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        try {
            Thread.sleep(milliseconds);
        } finally {
            for (BenchLoopWorker benchLoopWorker : benchLoopWorkers) {
                benchLoopWorker.stopThread();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }

        // Might throw an exception if the worker reported an error.
        for (BenchLoopWorker benchLoopWorker : benchLoopWorkers) {
            benchLoopWorker.getError();
        }

        final long endTime = System.currentTimeMillis();
        double elapsedTime = ((double) (endTime - startTime)) / 1000.0;

        if (!isWarmup) {
            new MemoryUsageStats("After benchmark", oakBench).printDataRow();
            System.out.println();
        }

        return collectIterationStats(benchLoopWorkers, elapsedTime);
    }

    public void iteration(int iteration) throws Exception {
        // Init the benchmark class and allocate resources
        oakBench.init();

        // Reset the incremental key producer for "increasing" key distribution.
        IncreasingKeyIndex.reset();

        // Make sure we start from a clean slate.
        System.gc();

        try {
            // Warmup iteration does not print statistics
            final boolean isWarmup = iteration < 0;

            if (isWarmup) {
                PrintTools.printHeader("Benchmark warmup");
            } else {
                PrintTools.printHeader("Benchmark iteration: %,d", iteration);
            }

            final int executeTime = isWarmup ? Parameters.confWarmupMilliseconds : Parameters.confNumMilliseconds;
            OpCounter.Stats s = execute(executeTime, isWarmup);

            if (isWarmup) {
                System.out.println("Warmup complete");
            }

            if (!isWarmup) {
                stats[iteration] = s;
                s.printStats();

                if (Parameters.confDetailedStats) {
                    oakBench.printMemStats();
                }
            }
        } finally {
            // Release the benchmark resources.
            oakBench.close();
        }
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

        OpCounter.Stats s = new OpCounter.Stats(Stream.of(stats).toArray(OpCounter.Stats[]::new));
        s.printStats();
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

    public static void main(String[] args) throws Exception {
        new Test(args).run();
    }
}
