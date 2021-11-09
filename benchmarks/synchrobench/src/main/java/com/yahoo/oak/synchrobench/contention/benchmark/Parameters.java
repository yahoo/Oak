/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public static int confNumFillThreads;
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

    public static String confScenario;

    // Pre configured scenarios
    public static final Map<String, String[]> SCENARIOS = Stream.of(new String[][]{
        {"4a-put", "-a", "0", "-u", "100"},
        {"4b-putIfAbsentComputeIfPresent", "--buffer", "-u", "0", "-s", "100", "-c"},
        {"4c-get-zc", "--buffer"},
        {"4c-get-copy"},
        {"4d-95Get5Put", "--buffer", "-a", "0", "-u", "5"},
        {"4e-entrySet-ascend", "--buffer", "-c"},
        {"4e-entryStreamSet-ascend", "--buffer", "-c", "--stream-iteration"},
        {"4f-entrySet-descend", "--buffer", "-c", "-a", "100"},
        {"4f-entryStreamSet-descend", "--buffer", "-c", "-a", "100", "--stream-iteration"},
        // sequential puts, doesn't need to be part of the regression
        {"not-random-put", "-a", "0", "-u", "100", "--inc"},
        {"50Pu50Delete", "-a", "50", "-u", "100"},
        {"25Put25Delete50Get", "-a", "25", "-u", "50"},
        {"05Put05Delete90Get", "-a", "05", "-u", "10"},
        {"50Pu50Delete_ZC", "-a", "50", "-u", "100", "--buffer"},
        {"25Put25Delete90Get_ZC", "-a", "25", "-u", "50", "--buffer"},
        {"05Put05Delete90Get_ZC", "-a", "05", "-u", "10", "--buffer"}
    }).collect(
        Collectors.toMap(data -> data[0], data -> Arrays.copyOfRange(data, 1, data.length))
    );

    static {
        resetToDefault();
    }

    public static void resetToDefault() {
        confSmallFootprint = false;

        confNumThreads = 1;
        confNumFillThreads = 1;
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

        confScenario = "none";
    }

    static void printHelpConf(String flag, String help) {
        System.out.printf("\t%-20s -- %-55s%n", flag, help);
    }

    static void printConf(String flag, String shortName, String help, String value, boolean isHelp) {
        if (isHelp) {
            System.out.printf("\t%-20s -- %-55s (default: %s)%n", flag, help, value);
        } else {
            System.out.printf("  %32s: %s%n", shortName, value);
        }
    }

    static void printConf(String flag, String shortName, String help, int value, boolean isHelp) {
        printConf(flag, shortName, help, String.format("%,d", value), isHelp);
    }

    static void printConf(String flag, String shortName, String help, boolean value, boolean isHelp) {
        printConf(flag, shortName, help, value ? "enabled" : "disabled", isHelp);
    }

    public static void print(boolean isHelp) {
        printConf("-e", "Detailed stats", "print detailed statistics",
            confDetailedStats, isHelp);
        printConf("-l", "Measure latency", "enables latency measurements",
            confMeasureLatency, isHelp);

        printConf("-t thread-num", "Number of threads", "set the number of threads",
            confNumThreads, isHelp);
        printConf("--fill-threads N", "Number of initialization threads",
            "set the number of threads for initialization",
            confNumFillThreads, isHelp);
        printConf("-d duration", "Duration", "set the duration of the benchmark, in milliseconds",
            confNumMilliseconds, isHelp);

        printConf("-u updates", "Write ratio", "set the percentage of updates",
            confNumWrites, isHelp);
        printConf("-a writeAll", "WriteAll ratio", "set the percentage of composite updates",
            confNumWriteAlls, isHelp);
        printConf("-s snapshot", "Snapshot ratio", "set the percentage of composite read-only operations",
            confNumSnapshots, isHelp);
        printConf("-c", "Change", "change the operation",
            confChange, isHelp);

        printConf("-i size", "Size", "set the data structure initial size",
            confSize, isHelp);
        printConf("-r range", "Range", "set the cardinality of the keys",
            confRange, isHelp);

        printConf("-n iterations", "Iterations", "set the bench iterations in the same JVM",
            confIterations, isHelp);
        printConf("-W warmup", "Warmup iterations", "set the JVM warmup duration, in milliseconds",
            confWarmupMilliseconds, isHelp);

        printConf("-b benchmark", "Benchmark", "set the benchmark class name",
            confKeySize, isHelp);
        printConf("--key key", "Key", "set the key class name",
            confKeySize, isHelp);
        printConf("--value val", "Value", "set the value class name",
            confValSize, isHelp);

        printConf("-k keySize", "Key size", "set the size of the keys, in Bytes",
            confKeySize, isHelp);
        printConf("-v valSize", "Value size", "set the size of the values, in Bytes",
            confValSize, isHelp);

        printConf("--consume-keys", "Consume keys", "enables key consumption",
            confConsumeKeys, isHelp);
        printConf("--consume-values", "Consume values", "enables value consumption",
            confConsumeValues, isHelp);

        printConf("--si", "Stream iteration", "enables stream iteration for scan",
            confStreamIteration, isHelp);
        printConf("--buffer", "Buffer view", "Use ZC interface when possible",
            confZeroCopy, isHelp);

        printConf("--small-footprint", "Small footprint", "Configure the map for a small footprint",
            confSmallFootprint, isHelp);

        printConf("--scenario", "Scenario", "set one of pre defined scenarios",
            confScenario, isHelp);
    }

    /**
     * Print the parameters that have been given as an input to the benchmark
     */
    public static void print() {
        print(false);
    }

    /**
     * Print the benchmark usage on the standard output
     */
    public static void printUsage() {
        resetToDefault();
        System.out.println("Usage:\n"
            + "java synchrobench.benchmark.Test [options] [-- stm-specific options]\n"
        );
        printHelpConf("-h", "print usage instructions and exit");
        printHelpConf("--print", "print parsed parameters and exit");
        print(true);
    }

    public static boolean isRandomKeyDistribution() {
        return confKeyDistribution == KeyDist.RANDOM;
    }

    /**
     * Parse the parameters on the command line
     */
    private static void parseParameters(String[] args) throws IllegalArgumentException {
        int argNumber = 0;

        while (argNumber < args.length) {
            String currentArg = args[argNumber++];

            try {
                switch (currentArg) {
                    case "--help":
                    case "-h":
                        printUsage();
                        System.exit(0);
                    case "--print":
                        print();
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
                    case "--fill-threads":
                        confNumFillThreads = parseInt(args[argNumber++]);
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
                    case "--scenario":
                        confScenario = args[argNumber++];
                        parseParameters(SCENARIOS.get(confScenario));
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
    }

    /**
     * Parse the parameters on the command line
     */
    public static void parseCommandLineParameters(String[] args) throws IllegalArgumentException {
        resetToDefault();
        parseParameters(args);
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
