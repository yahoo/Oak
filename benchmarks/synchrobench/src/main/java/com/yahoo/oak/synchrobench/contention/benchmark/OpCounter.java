/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchOp;
import org.apache.datasketches.kll.KllFloatsSketch;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class OpCounter {
    public static final float NS = 1e-9f;
    public static final int NUM_OPS = BenchOp.values().length;

    protected final long[] success = new long[NUM_OPS];
    protected final long[] failed = new long[NUM_OPS];
    protected final KllFloatsSketch[] successLatency = new KllFloatsSketch[NUM_OPS];
    protected final KllFloatsSketch[] failedLatency = new KllFloatsSketch[NUM_OPS];

    public OpCounter() {
        reset();
    }

    public OpCounter(OpCounter[] counters) {
        this();
        for (OpCounter c : counters) {
            add(c);
        }
    }

    void reset() {
        for (int i = 0; i < NUM_OPS; i++) {
            success[i] = 0;
            failed[i] = 0;
            successLatency[i] = new KllFloatsSketch();
            failedLatency[i] = new KllFloatsSketch();
        }
    }

    void countOp(BenchOp op, boolean successful) {
        final int i = op.ordinal();
        if (successful) {
            success[i] += 1;
        } else {
            failed[i] += 1;
        }
    }

    void countOp(BenchOp op, boolean successful, long latencyNano) {
        final int i = op.ordinal();
        final float latencySec = latencyNano * NS;
        if (successful) {
            success[i] += 1;
            successLatency[i].update(latencySec);
        } else {
            failed[i] += 1;
            failedLatency[i].update(latencySec);
        }
    }

    void add(OpCounter other) {
        for (int i = 0; i < NUM_OPS; i++) {
            this.success[i] += other.success[i];
            this.failed[i] += other.failed[i];
            this.successLatency[i].merge(other.successLatency[i]);
            this.failedLatency[i].merge(other.failedLatency[i]);
        }
    }

    public long success(BenchOp op) {
        return success[op.ordinal()];
    }

    public long failed(BenchOp op) {
        return failed[op.ordinal()];
    }

    public long total(BenchOp op) {
        return success(op) + failed(op);
    }

    public long total() {
        return success() + failed();
    }

    public long success() {
        return LongStream.of(success).sum();
    }

    public long failed() {
        return LongStream.of(failed).sum();
    }

    public long total(BenchOp[] ops) {
        return success(ops) + failed(ops);
    }

    public long success(BenchOp[] ops) {
        return Stream.of(ops).mapToLong(op -> success[op.ordinal()]).sum();
    }

    public long failed(BenchOp[] ops) {
        return Stream.of(ops).mapToLong(op -> failed[op.ordinal()]).sum();
    }

    public double opRate(BenchOp op) {
        return (double) total(op) * 100. / (double) total();
    }

    public double opRate(BenchOp[] ops) {
        return (double) total(ops) * 100. / (double) total();
    }

    public double successRate() {
        return (double) success() * 100. / (double) total();
    }

    public double failedRate() {
        return (double) failed() * 100. / (double) total();
    }

    public double successRate(BenchOp op) {
        return (double) success(op) * 100. / (double) total();
    }

    public double failedRate(BenchOp op) {
        return (double) failed(op) * 100. / (double) total();
    }

    public double successRate(BenchOp[] ops) {
        return (double) success(ops) * 100. / (double) total();
    }

    public double failedRate(BenchOp[] ops) {
        return (double) failed(ops) * 100. / (double) total();
    }

    public long successLatency(BenchOp op, double fraction) {
        return (long) (successLatency[op.ordinal()].getQuantile(fraction) / NS);
    }

    public long failedLatency(BenchOp op, double fraction) {
        return (long) (failedLatency[op.ordinal()].getQuantile(fraction) / NS);
    }

    public long successLatency(BenchOp[] ops, double fraction) {
        final KllFloatsSketch agg = new KllFloatsSketch();
        Stream.of(ops).map(op -> successLatency[op.ordinal()]).forEach(agg::merge);
        return (long) (agg.getQuantile(fraction) / NS);
    }

    public long failedLatency(BenchOp[] ops, double fraction) {
        final KllFloatsSketch agg = new KllFloatsSketch();
        Stream.of(ops).map(op -> failedLatency[op.ordinal()]).forEach(agg::merge);
        return (long) (agg.getQuantile(fraction) / NS);
    }

    public long successLatency(double fraction) {
        final KllFloatsSketch agg = new KllFloatsSketch();
        Stream.of(successLatency).forEach(agg::merge);
        return (long) (agg.getQuantile(fraction) / NS);
    }

    public long failedLatency(double fraction) {
        final KllFloatsSketch agg = new KllFloatsSketch();
        Stream.of(failedLatency).forEach(agg::merge);
        return (long) (agg.getQuantile(fraction) / NS);
    }

    private void printOpsStatsLine(String title, BenchOp[] ops) {
        System.out.printf("  %32s | %,12d (%8.2f %%) | %12.2f %%%n", title, total(ops), opRate(ops), successRate(ops));
    }

    public void printOpsStats() {
        System.out.printf("  %32s | %12s (%8s %%) | %6s %%%n", "Operation", "Count", "Of Total", "Success Rate");
        System.out.println(PrintTools.dashLine(81));

        for (Map.Entry<String, BenchOp[]> i : BenchOp.GROUPS.entrySet()) {
            if (total(i.getValue()) > 0) {
                printOpsStatsLine(i.getKey(), i.getValue());
            }
        }

        System.out.println();

        if (Parameters.confMeasureLatency) {
            printLatencyStats();
        }
    }

    private void printLatencyStatsLine(String title, BenchOp[] ops) {
        System.out.printf("  %32s | %,7d | %,7d | %,7d | %,10d | %,10d | %,7d | %,7d | %,7d | %,10d | %,10d |%n",
            title,
            successLatency(ops, 0.), successLatency(ops, 0.5),
            successLatency(ops, 0.99), successLatency(ops, 0.999),
            successLatency(ops, 1.),
            failedLatency(ops, 0.), failedLatency(ops, 0.5),
            failedLatency(ops, 0.99),  failedLatency(ops, 0.999),
            failedLatency(ops, 1.));
    }

    public void printLatencyStats() {
        System.out.printf("  %32s |                 Success Latency (ns)                  |" +
            "                   Fail Latency (ns)                   |%n", "");
        System.out.printf("  %32s | %7s | %7s | %7s | %10s | %10s | %7s | %7s | %7s | %10s | %10s |%n",
            "Operation", "MIN",  "p50", "p99", "p99.9", "MAX", "MIN",  "p50", "p99", "p99.9", "MAX");
        System.out.println(PrintTools.dashLine(148));

        for (Map.Entry<String, BenchOp[]> i : BenchOp.GROUPS.entrySet()) {
            if (total(i.getValue()) > 0) {
                printLatencyStatsLine(i.getKey(), i.getValue());
            }
        }

        System.out.println();
    }

    public static class Stats extends OpCounter {
        final double time;
        final double numThreads;
        final double totalSize;

        public Stats(
            OpCounter[] counters,
            double time,
            int numThreads,
            int totalSize
        ) {
            super(counters);
            this.time = time;
            this.numThreads = numThreads;
            this.totalSize = totalSize;
        }

        public Stats(Stats[] stats) {
            super(stats);
            this.time = Stream.of(stats).mapToDouble(s -> s.time).sum();
            this.numThreads = Stream.of(stats).mapToDouble(s -> s.numThreads).average().getAsDouble();
            this.totalSize = Stream.of(stats).mapToDouble(s -> s.totalSize).average().getAsDouble();
        }

        public double avgThroughput() {
            return (double) total() / time;
        }

        public double avgLatency()  {
            return (double) numThreads / avgThroughput();
        }

        public void printStats() {
            PrintTools.printHeader("Benchmark statistics");
            System.out.printf("  %22s: %,.2f%n", "Elapsed time (s)", time);
            System.out.printf("  %22s: %,.0f%n", "Final size", totalSize);
            System.out.printf("  %22s: %,d%n", "Expected size",
                Parameters.confSize + success(BenchOp.ADD) - success(BenchOp.REMOVE));
            System.out.printf("  %22s: %,.2f%n", "Throughput (mebiops/s)", avgThroughput() / (double) (1 << 20));
            System.out.printf("  %22s: %,d%n", "Average Latency (ns)", (long) (avgLatency() / OpCounter.NS));
            System.out.println();

            printOpsStats();
        }
    }
}
