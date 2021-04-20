/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.MyBuffer;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalOakMap;

import java.lang.reflect.Method;
import java.util.Random;

/**
 * The loop executed by each thread of the map
 * benchmark.
 *
 * @author Vincent Gramoli
 */
public class ThreadLoopOak implements Runnable {

    /**
     * The instance of the running benchmark
     */
    CompositionalOakMap<MyBuffer, MyBuffer> bench;
    /**
     * The stop flag, indicating whether the loop is over
     */
    protected volatile boolean stop = false;
    /**
     * The pool of methods that can run
     */
    protected Method[] methods;
    /**
     * The number of the current thread
     */
    protected final short myThreadNum;

    /**
     * The counters of the thread successful operations
     */
    long numAdd = 0;
    long numRemove = 0;
    long numAddAll = 0;
    long numRemoveAll = 0;
    long numSize = 0;
    long numContains = 0;
    /**
     * The counter of the false-returning operations
     */
    long failures = 0;
    /**
     * The counter of the thread operations
     */
    long total = 0;
    /**
     * The counter of aborts
     */
    long aborts = 0;
    /**
     * The random number
     */
    Random rand = new Random();

    long getCount;
    long nodesTraversed;
    long structMods;

    /**
     * The distribution of methods as an array of percentiles
     * <p>
     * 0%        cdf[0]        cdf[2]                     100%
     * |--writeAll--|--writeSome--|--readAll--|--readSome--|
     * |-----------write----------|--readAll--|--readSome--| cdf[1]
     */
    int[] cdf = new int[3];

    public ThreadLoopOak(short myThreadNum,
                         CompositionalOakMap<MyBuffer, MyBuffer> bench, Method[] methods) {
        this.myThreadNum = myThreadNum;
        this.bench = bench;
        this.methods = methods;
        /* initialize the method boundaries */
        assert (Parameters.confNumWrites >= Parameters.confNumWriteAlls);
        cdf[0] = 10 * Parameters.confNumWriteAlls;
        cdf[1] = 10 * Parameters.confNumWrites;
        cdf[2] = cdf[1] + 10 * Parameters.confNumSnapshots;
    }

    public void stopThread() {
        stop = true;
    }

    public void run() {

        boolean change = Parameters.confChange;
        int scanLength = 10000;


        MyBuffer key = new MyBuffer(Parameters.confKeySize);

        // for the key distribution INCREASING we want to continue the increasing integers sequence,
        // started in the initial filling of the map
        // for the key distribution RANDOM the below value will be overwritten anyway
        int newInt = Parameters.confSize;

        while (!stop) {
            newInt = (Parameters.confKeyDistribution == Parameters.KeyDist.RANDOM) ?
                    rand.nextInt(Parameters.confRange) : newInt + 1;
            key.buffer.putInt(0, newInt);

            int coin = rand.nextInt(1000);
            if (coin < cdf[0]) { // -a
                if (!change) {
                    bench.removeOak(key);
                    numRemove++;
                } else {
                    if (bench.descendOak(key, scanLength)) {
                        numRemoveAll++;
                    } else {
                        failures++;
                    }
                }
            } else if (coin < cdf[1]) { // -u
                MyBuffer newKey = new MyBuffer(Parameters.confKeySize);
                MyBuffer newVal = new MyBuffer(Parameters.confValSize);
                newKey.buffer.putInt(0, newInt);
                newVal.buffer.putInt(0, newInt);
                if (!change) {
                    bench.putOak(newKey, newVal);
                    numAdd++;
                } else {
                    if (bench.putIfAbsentOak(newKey, newVal)) {
                        numAdd++;
                    } else {
                        failures++;
                    }
                }
            } else if (coin < cdf[2]) { // -s
                if (!change) {
                    bench.computeOak(key);
                    numSize++;
                } else {
                    MyBuffer newKey = new MyBuffer(Parameters.confKeySize);
                    MyBuffer newVal = new MyBuffer(Parameters.confValSize);
                    newKey.buffer.putInt(0, newInt);
                    newVal.buffer.putInt(0, newInt);
                    bench.putIfAbsentComputeIfPresentOak(newKey, newVal);
                    numSize++;
                }
            } else {
                if (!change) {
                    if (bench.getOak(key)) {
                        numContains++;
                    } else {
                        failures++;
                    }
                } else {
                    if (bench.ascendOak(key, scanLength)) {
                        numContains++;
                    } else {
                        failures++;
                    }
                }
            }

            total++;

            assert total == failures + numContains + numSize + numRemove
                    + numAdd + numRemoveAll + numAddAll;
        }
    }

}
