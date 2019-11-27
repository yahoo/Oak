package com.oath.oak.synchrobench.contention.benchmark;


import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.oath.oak.synchrobench.maps.MyBuffer;

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
    public CompositionalOakMap<MyBuffer, MyBuffer> bench;
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
    public long numAdd = 0;
    public long numRemove = 0;
    public long numAddAll = 0;
    public long numRemoveAll = 0;
    public long numSize = 0;
    public long numContains = 0;
    /**
     * The counter of the false-returning operations
     */
    public long failures = 0;
    /**
     * The counter of the thread operations
     */
    public long total = 0;
    /**
     * The counter of aborts
     */
    public long aborts = 0;
    /**
     * The random number
     */
    Random rand = new Random();

    public long getCount;
    public long nodesTraversed;
    public long structMods;

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
        assert (Parameters.numWrites >= Parameters.numWriteAlls);
        cdf[0] = 10 * Parameters.numWriteAlls;
        cdf[1] = 10 * Parameters.numWrites;
        cdf[2] = cdf[1] + 10 * Parameters.numSnapshots;
    }

    public void stopThread() {
        stop = true;
    }

    public void run() {

        boolean change = Parameters.change;
        int size = 10000;


        MyBuffer key = new MyBuffer(Parameters.keySize);

        Integer newInt = -1;
        while (!stop) {
            newInt = (Parameters.keyDistribution == Parameters.KeyDist.RANDOM) ? rand.nextInt(Parameters.range) :
                    newInt + 1;
            key.buffer.putInt(0, newInt);

            int coin = rand.nextInt(1000);
            if (coin < cdf[0]) { // -a
                if (!change) {
                    bench.removeOak(key);
                    numRemove++;
                } else {
                    if (bench.descendOak(key, size)) {
                        numRemoveAll++;
                    } else {
                        failures++;
                    }
                }
            } else if (coin < cdf[1]) { // -u
                MyBuffer newKey = new MyBuffer(Parameters.keySize);
                MyBuffer newVal = new MyBuffer(Parameters.valSize);
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
                    MyBuffer newKey = new MyBuffer(Parameters.keySize);
                    MyBuffer newVal = new MyBuffer(Parameters.valSize);
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
                    if (bench.ascendOak(key, size)) {
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
