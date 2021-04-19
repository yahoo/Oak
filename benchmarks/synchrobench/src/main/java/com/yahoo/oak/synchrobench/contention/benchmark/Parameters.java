/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

/**
 * Parameters of the Java version of the
 * Synchrobench benchmark.
 *
 * @author Vincent Gramoli
 */
public class Parameters {
    enum KeyDist {
        RANDOM,
        INCREASING
    }

    public static int confNumThreads = 1;
    public static int confNumMilliseconds = 5000;
    public static int confNumWrites = 0;
    public static int confNumWriteAlls = 0;
    public static int confNumSnapshots = 0;
    public static int confRange = 2048;
    public static int confSize = 1024;
    public static int confWarmUp = 5;
    public static int confIterations = 1;
    public static int confKeySize = 4;
    public static int confValSize = 4;

    public static boolean confDetailedStats = false;
    static boolean confChange = false;
    public static boolean confStreamIteration = false;

    public static boolean confZeroCopy = false;

    public static KeyDist confKeyDistribution = KeyDist.RANDOM;

    public static String confBenchClassName = "com.yahoo.oak.synchrobench.maps.OakMap";

    public static String asString() {
        return "Parameters:\n"
                + "\t-e            -- print detailed statistics (default: "
                + Parameters.confDetailedStats
                + ")\n"
                + "\t-t thread-num -- set the number of threads (default: "
                + Parameters.confNumThreads
                + ")\n"
                + "\t-d duration   -- set the length of the benchmark, in milliseconds (default: "
                + Parameters.confNumMilliseconds
                + ")\n"
                + "\t-u updates    -- set the number of threads (default: "
                + Parameters.confNumWrites
                + ")\n"
                + "\t-a writeAll   -- set the percentage of composite updates (default: "
                + Parameters.confNumWriteAlls
                + ")\n"
                + "\t-s snapshot   -- set the percentage of composite read-only operations (default: "
                + Parameters.confNumSnapshots
                + ")\n"
                + "\t-r range      -- set the element range (default: "
                + Parameters.confRange
                + ")\n"
                + "\t-b benchmark  -- set the benchmark (default: "
                + Parameters.confBenchClassName
                + ")\n"
                + "\t-i size       -- set the datastructure initial size (default: "
                + Parameters.confSize
                + ")\n"
                + "\t-n iterations -- set the bench iterations in the same JVM (default: "
                + Parameters.confIterations
                + ")\n"
                + "\t-k keySize    -- set the size of the keys, in Bytes (default: "
                + Parameters.confKeySize
                + ")\n"
                + "\t-v valSize    -- set the size of the values, in Bytes (default: "
                + Parameters.confValSize
                + ")\n"
                + "\t-c changeOp   -- change the operation (default: "
                + Parameters.confChange
                + ")\n"
                + "\t-W warmup     -- set the JVM warmup length, in seconds (default: "
                + Parameters.confWarmUp + ").";
    }
}
