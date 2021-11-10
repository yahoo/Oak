/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchOp;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalMap;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

/**
 * The loop executed by each thread of the map
 * benchmark.
 *
 * @author Vincent Gramoli
 */
public class BenchLoopWorker implements Runnable {

    /**
     * The instance of the running benchmark
     */
    CompositionalMap bench;
    KeyGenerator keyGen;
    ValueGenerator valueGen;

    BenchKey lastKey;

    /**
     * The stop flag, indicating whether the loop is over
     */
    protected volatile boolean stop = false;

    protected final OpCounter counter = new OpCounter();
    /**
     * The random number
     */
    final Random coinRand;
    final Random keyRand;
    final Random valueRand;

    Exception error = null;

    /**
     * The distribution of methods as an array of percentiles
     * <p>
     * 0%        cdf[0]        cdf[2]                     100%
     * |--writeAll--|--writeSome--|--readAll--|--readSome--|
     * |-----------write----------|--readAll--|--readSome--| cdf[1]
     */
    int[] cdf = new int[3];

    public BenchLoopWorker(
        CompositionalMap bench,
        KeyGenerator keyGen,
        ValueGenerator valueGen,
        BenchKey lastKey
    ) {
        this.bench = bench;
        this.keyGen = keyGen;
        this.valueGen = valueGen;
        this.coinRand = new Random();
        this.valueRand = new Random();
        this.lastKey = lastKey;

        // for the key distribution INCREASING we want to continue the increasing integers sequence,
        // started in the initial filling of the map
        // for the key distribution RANDOM the below value will be overwritten anyway
        this.keyRand = Parameters.isRandomKeyDistribution() ? this.valueRand : null;

        /* initialize the method boundaries */
        cdf[0] = 10 * Parameters.confNumWriteAlls;
        cdf[1] = 10 * Parameters.confNumWrites;
        cdf[2] = cdf[1] + 10 * Parameters.confNumSnapshots;
    }

    public void stopThread() {
        stop = true;
    }

    @Override
    public void run() {
        final Blackhole blackhole = new Blackhole(
            "Today's password is swordfish. " +
            "I understand instantiating Blackholes directly is dangerous."
        );

        try {
            if (Parameters.confMeasureLatency) {
                runWithLatency(blackhole);
            } else {
                runWithoutLatency(blackhole);
            }
        } catch (UnsupportedOperationException e) {
            System.err.printf("Tried to test an unsupported operation: %s%n", e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.printf("Failed during execution: %s%n", e.getMessage());
            error = e;
        } finally {
            blackhole.evaporate(
                "Yes, I am Stephen Hawking, and know a thing or two about black holes."
            );
        }
    }

    enum Coin {
        WRITE_ALL,
        WRITE,
        SNAPSHOT,
        OTHER
    }

    private Coin flip() {
        final int coin = coinRand.nextInt(1000);
        if (coin < cdf[0]) { // -a
            return Coin.WRITE_ALL;
        } else if (coin < cdf[1]) { // -u
            return Coin.WRITE;
        } else if (coin < cdf[2]) { // -s
            return Coin.SNAPSHOT;
        } else {
            return Coin.OTHER;
        }
    }

    private BenchOp getRandOp() {
        final Coin coin = flip();
        if (Parameters.confChange) {
            switch (coin) {
                case WRITE_ALL:
                    return BenchOp.DESCEND;
                case WRITE:
                    return BenchOp.PUT_IF_ABSENT;
                case SNAPSHOT:
                    return BenchOp.PUT_IF_ABSENT_COMPUTE_IF_PRESENT;
                case OTHER:
                    return BenchOp.ASCEND;
            }
        } else {
            switch (coin) {
                case WRITE_ALL:
                    return BenchOp.REMOVE;
                case WRITE:
                    return BenchOp.PUT;
                case SNAPSHOT:
                    return BenchOp.COMPUTE;
                case OTHER:
                    return BenchOp.GET;
            }
        }

        throw new IllegalStateException(String.format("Coin failed: %s", coin));
    }

    private BenchKey nextKey() {
        BenchKey curKey = keyGen.getNextKey(keyRand, Parameters.confRange, lastKey);
        lastKey = curKey;
        return curKey;
    }

    private BenchValue nextValue() {
        return valueGen.getNextValue(valueRand, Parameters.confRange);
    }

    private boolean doOp(BenchOp op, Blackhole blackhole) {
        switch (op) {
            case GET:
                return bench.getOak(nextKey(), blackhole);
            case PUT:
                bench.putOak(nextKey(), nextValue());
                return true;
            case PUT_IF_ABSENT:
                return bench.putIfAbsentOak(nextKey(), nextValue());
            case PUT_IF_ABSENT_COMPUTE_IF_PRESENT:
                bench.putIfAbsentComputeIfPresentOak(nextKey(), nextValue());
                return true;
            case REMOVE:
                bench.removeOak(nextKey());
                return true;
            case COMPUTE:
                bench.computeOak(nextKey());
                return true;
            case ASCEND:
                return bench.ascendOak(nextKey(), Parameters.confScanLength, blackhole);
            case DESCEND:
                return bench.descendOak(nextKey(), Parameters.confScanLength, blackhole);
        }

        return false;
    }

    private void runWithoutLatency(Blackhole blackhole) {
        while (!stop) {
            BenchOp op = getRandOp();
            boolean successful = doOp(op, blackhole);
            counter.countOp(op, successful);
        }
    }

    private void runWithLatency(Blackhole blackhole) {
        while (!stop) {
            BenchOp op = getRandOp();
            final long start = System.nanoTime();
            final boolean successful = doOp(op, blackhole);
            final long end = System.nanoTime();
            counter.countOp(op, successful, end - start);
        }
    }
}
