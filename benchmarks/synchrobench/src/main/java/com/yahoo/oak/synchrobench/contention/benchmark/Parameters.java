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
    public enum KeyDist {
        RANDOM,
        INCREASING
    }

    public static boolean confSmallFootprint;

    public static int confNumThreads;
    public static int confNumMilliseconds;
    public static int confNumWrites;
    public static int confNumWriteAlls;
    public static int confNumSnapshots;
    public static int confRange;
    public static int confSize;
    public static int confWarmupMilliseconds;
    public static int confIterations;
    public static int confKeySize;
    public static int confValSize;

    public static boolean confChange;
    public static boolean confStreamIteration;
    public static boolean confZeroCopy;
    public static boolean confConsumeKeys;
    public static boolean confConsumeValues;

    public static int confScanLength;

    public static KeyDist confKeyDistribution;

    public static String confBenchClass;
    public static String confKeyClass;
    public static String confValueClass;

    public static boolean confMeasureLatency;
    public static boolean confDetailedStats;

    static {
        resetToDefault();
    }

    public static void resetToDefault() {
        confSmallFootprint = false;

        confNumThreads = 1;
        confNumMilliseconds = 5000;
        confNumWrites = 0;
        confNumWriteAlls = 0;
        confNumSnapshots = 0;
        confRange = 2048;
        confSize = 1024;
        confWarmupMilliseconds = 1000;
        confIterations = 1;
        confKeySize = 4;
        confValSize = 4;

        confChange = false;
        confStreamIteration = false;
        confZeroCopy = false;
        confConsumeKeys = false;
        confConsumeValues = false;

        confScanLength = 1000;

        confKeyDistribution = KeyDist.RANDOM;

        confBenchClass = "com.yahoo.oak.synchrobench.maps.OakMap";
        confKeyClass = "com.yahoo.oak.synchrobench.data.buffer.MyBuffer";
        confValueClass = "com.yahoo.oak.synchrobench.data.buffer.MyBuffer";

        confMeasureLatency = false;
        confDetailedStats = false;
    }

    public static String asString() {
        resetToDefault();
        return "Parameters:\n"
                + "\t-e               -- print detailed statistics (default: "
                + (confDetailedStats ? "enabled" : "disabled")
                + ")\n"
                + "\t-l               -- enables latency measurements (default: "
                + (confMeasureLatency ? "enabled" : "disabled")
                + ")\n"
                + "\t-t thread-num    -- set the number of threads (default: "
                + confNumThreads
                + ")\n"
                + "\t-d duration      -- set the length of the benchmark, in milliseconds (default: "
                + confNumMilliseconds
                + ")\n"
                + "\t-u updates       -- set the number of threads (default: "
                + confNumWrites
                + ")\n"
                + "\t-a writeAll      -- set the percentage of composite updates (default: "
                + confNumWriteAlls
                + ")\n"
                + "\t-s snapshot      -- set the percentage of composite read-only operations (default: "
                + confNumSnapshots
                + ")\n"
                + "\t-r range         -- set the cardinality of the keys (default: "
                + confRange
                + ")\n"
                + "\t-b benchmark     -- set the benchmark (default: "
                + confBenchClass
                + ")\n"
                + "\t-i size          -- set the data structure initial size (default: "
                + confSize
                + ")\n"
                + "\t-n iterations    -- set the bench iterations in the same JVM (default: "
                + confIterations
                + ")\n"
                + "\t-k keySize       -- set the size of the keys, in Bytes (default: "
                + confKeySize
                + ")\n"
                + "\t-v valSize       -- set the size of the values, in Bytes (default: "
                + confValSize
                + ")\n"
                + "\t-c changeOp      -- change the operation (default: "
                + confChange
                + ")\n"
                + "\t-W warmup        -- set the JVM warmup length, in milliseconds (default: "
                + confWarmupMilliseconds
                + ")\n"
                + "\t--consume-keys   -- enables key consumption (default: "
                + (confConsumeKeys ? "enabled" : "disabled")
                + ")\n"
                + "\t--consume-values -- enables value consumption (default: "
                + (confConsumeValues ? "enabled" : "disabled") + ").";
    }

    /**
     * Print the parameters that have been given as an input to the benchmark
     */
    public static void print() {
        String params = "Benchmark parameters" + "\n"
            + "--------------------" + "\n"
            + "  Detailed stats:          \t"
            + (Parameters.confDetailedStats ? "enabled" : "disabled") + "\n"
            + "  Measure latency          \t"
            + (Parameters.confMeasureLatency ? "enabled" : "disabled") + "\n"
            + "  Number of threads:       \t"
            + Parameters.confNumThreads + "\n"
            + "  Length:                  \t"
            + Parameters.confNumMilliseconds + " ms\n"
            + "  Write ratio:             \t"
            + Parameters.confNumWrites + " %\n"
            + "  WriteAll ratio:          \t"
            + Parameters.confNumWriteAlls + " %\n"
            + "  Snapshot ratio:          \t"
            + Parameters.confNumSnapshots + " %\n"
            + "  Size:                    \t"
            + String.format("%,d", Parameters.confSize) + " elts\n"
            + "  Range:                   \t"
            + String.format("%,d", Parameters.confRange) + " elts\n"
            + "  WarmUp:                  \t"
            + Parameters.confWarmupMilliseconds + " s\n"
            + "  Iterations:              \t"
            + Parameters.confIterations + "\n"
            + "  Key size:              \t"
            + Parameters.confKeySize + " Bytes\n"
            + "  Val size:              \t"
            + Parameters.confValSize + " Bytes\n"
            + "  Change:                \t"
            + Parameters.confChange + "\n"
            + "  Buffer view:            \t"
            + Parameters.confZeroCopy + "\n"
            + "  Benchmark:              \t"
            + Parameters.confBenchClass + "\n"
            + "  Key:                    \t"
            + Parameters.confKeyClass + "\n"
            + "  Value:                  \t"
            + Parameters.confValueClass + "\n"
            + "  Consume keys:           \t"
            + (Parameters.confConsumeKeys ? "enabled" : "disabled") + "\n"
            + "  Consume values:         \t"
            + (Parameters.confConsumeValues ? "enabled" : "disabled") + "\n";
        System.out.print(params);
    }

    /**
     * Print the benchmark usage on the standard output
     */
    public static void printUsage() {
        String syntax = "Usage:\n"
            + "java synchrobench.benchmark.Test [options] [-- stm-specific options]\n\n"
            + asString();
        System.err.println(syntax);
    }

    /**
     * Parse the parameters on the command line
     */
    public static void parseCommandLineParameters(String[] args) throws IllegalArgumentException {
        resetToDefault();

        int argNumber = 0;

        while (argNumber < args.length) {
            String currentArg = args[argNumber++];

            try {
                switch (currentArg) {
                    case "--help":
                    case "-h":
                        printUsage();
                        System.exit(0);
                    case "--verbose":
                    case "-e":
                        confDetailedStats = true;
                        break;
                    case "--latency":
                    case "-l":
                        confMeasureLatency = true;
                        break;
                    case "--small-footprint":
                        confSmallFootprint = true;
                        break;
                    case "--change":
                    case "-c":
                        confChange = true;
                        break;
                    case "--stream-iteration":
                    case "-si":
                        confStreamIteration = true;
                        break;
                    case "--buffer":
                        confZeroCopy = true;
                        break;
                    case "--inc":
                        confKeyDistribution = Parameters.KeyDist.INCREASING;
                        break;
                    case "--thread-nums":
                    case "-t":
                        confNumThreads = parseInt(args[argNumber++]);
                        break;
                    case "--duration":
                    case "-d":
                        confNumMilliseconds = parseInt(args[argNumber++]);
                        break;
                    case "--updates":
                    case "-u":
                        confNumWrites = parseInt(args[argNumber++]);
                        break;
                    case "--writeAll":
                    case "-a":
                        confNumWriteAlls = parseInt(args[argNumber++]);
                        break;
                    case "--consume-keys":
                        confConsumeKeys = true;
                        break;
                    case "--consume-values":
                        confConsumeValues = true;
                        break;
                    case "--snapshots":
                    case "-s":
                        confNumSnapshots = parseInt(args[argNumber++]);
                        break;
                    case "--size":
                    case "-i":
                        confSize = parseInt(args[argNumber++]);
                        break;
                    case "--range":
                    case "-r":
                        confRange = parseInt(args[argNumber++]);
                        break;
                    case "--Warmup":
                    case "-W":
                        confWarmupMilliseconds = parseInt(args[argNumber++]);
                        break;
                    case "--benchmark":
                    case "-b":
                        confBenchClass = args[argNumber++];
                        break;
                    case "--iterations":
                    case "-n":
                        confIterations = parseInt(args[argNumber++]);
                        break;
                    case "--key":
                        confKeyClass = args[argNumber++];
                        break;
                    case "--value":
                        confValueClass = args[argNumber++];
                        break;
                    case "--keySize":
                    case "-k":
                        confKeySize = parseInt(args[argNumber++]);
                        break;
                    case "--valSize":
                    case "-v":
                        confValSize = parseInt(args[argNumber++]);
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("Unknown parameter: %s", currentArg));
                }
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(String.format("Missing value after option: %s", currentArg));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format("Number expected after option: %s", currentArg));
            }
        }
        assert (confRange >= confSize);
        if (confRange != 2 * confSize) {
            System.err.println("Note that the value range is not twice the initial size, thus the size " +
                "expectation varies at runtime.");
        }
    }

    public static int parseInt(String number) {
        return Integer.parseInt(number.replace("_", "").replace(",", ""));
    }
}
