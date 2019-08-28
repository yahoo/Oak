package com.oath.oak.synchrobench.contention.benchmark;

import com.oath.oak.synchrobench.contention.abstractions.CompositionalMap;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.oath.oak.synchrobench.contention.abstractions.MaintenanceAlg;
import com.oath.oak.synchrobench.maps.MyBuffer;
import com.oath.oak.synchrobench.maps.OakMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Formatter;
import java.util.Locale;
import java.util.Random;

/**
 * Synchrobench-java, a benchmark to evaluate the implementations of
 * high level abstractions including Map and Set.
 *
 * @author Vincent Gramoli
 */
public class Test {

    public enum Type {
        OAKMAP
    }

    /**
     * The array of threads executing the benchmark
     */
    private Thread[] threads;
    /**
     * The array of runnable thread codes
     */
    private ThreadLoopOak[] threadLoopsOak;
    /**
     * The observed duration of the benchmark
     */
    private double elapsedTime;
    /**
     * The throughput
     */
    private double[] throughput = null;
    /**
     * Element count
     */
    private int[] totalSize = null;
    /**
     * The iteration
     */
    private int currentIteration = 0;

    /**
     * The total number of operations for all threads
     */
    private long total = 0;
    /**
     * The total number of successful operations for all threads
     */
    private long numAdd = 0;
    private long numRemove = 0;
    private long numAddAll = 0;
    private long numRemoveAll = 0;
    private long numSize = 0;
    private long numContains = 0;
    /**
     * The total number of failed operations for all threads
     */
    private long failures = 0;
    /**
     * The total number of aborts
     */
    private long aborts = 0;
    /**
     * The instance of the benchmark
     */
    private Type benchType = null;
    private CompositionalOakMap<MyBuffer, MyBuffer> oakBench = null;
    /** The instance of the benchmark */
    /**
     * The benchmark methods
     */
    private Method[] methods;

    private long nodesTraversed;
    public long structMods;
    private long getCount;

    /**
     * The thread-private PRNG
     */
    final private static ThreadLocal<Random> s_random = new ThreadLocal<Random>() {
        @Override
        protected synchronized Random initialValue() {
            return new Random();
        }
    };

    public long fill(final int range, final long size) {
        long operations = 0;
        for (long i = size; i > 0; ) {
            Integer v = s_random.get().nextInt(range);
            switch (benchType) {
                case OAKMAP:
                    MyBuffer key = new MyBuffer(Parameters.keySize);
                    key.buffer.putInt(0, v);
                    MyBuffer val = new MyBuffer(Parameters.valSize);
                    val.buffer.putInt(0, v);
                    if (oakBench.putIfAbsentOak(key, val)) {
                        i--;
                    }
                    operations++;
                    break;
                default:
                    System.err.println("Wrong benchmark type");
                    System.exit(0);
            }
        }
        return operations;
    }


    /**
     * Instantiate abstraction
     */
    @SuppressWarnings("unchecked")
    public void instanciateAbstraction(
            String benchName) {
        try {
            Class<CompositionalMap<Integer, Integer>> benchClass = (Class<CompositionalMap<Integer, Integer>>) Class
                    .forName(benchName);
            Constructor<CompositionalMap<Integer, Integer>> c = benchClass
                    .getConstructor();
            methods = benchClass.getDeclaredMethods();

            if (CompositionalOakMap.class.isAssignableFrom(benchClass)) {
                oakBench = (CompositionalOakMap<MyBuffer, MyBuffer>) c.newInstance();
                benchType = Type.OAKMAP;
            }

        } catch (Exception e) {
            System.err.println("Cannot find benchmark class: " + benchName);
            System.exit(-1);
        }
    }


    /**
     * Creates as many threads as requested
     *
     * @throws InterruptedException if unable to launch them
     */
    private void initThreads() throws InterruptedException {
        switch (benchType) {
            case OAKMAP:
                threadLoopsOak = new ThreadLoopOak[Parameters.numThreads];
                threads = new Thread[Parameters.numThreads];
                for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
                    threadLoopsOak[threadNum] = new ThreadLoopOak(threadNum, oakBench, methods);
                    threads[threadNum] = new Thread(threadLoopsOak[threadNum]);
                }
                break;
        }
    }

    /**
     * Constructor sets up the benchmark by reading parameters and creating
     * threads
     *
     * @param args the arguments of the command-line
     */
    public Test(String[] args) {
        printHeader();
        try {
            parseCommandLineParameters(args);
        } catch (Exception e) {
            System.err.println("Cannot parse parameters.");
            e.printStackTrace();
        }
        instanciateAbstraction(Parameters.benchClassName);
        this.throughput = new double[Parameters.iterations];
        this.totalSize = new int[Parameters.iterations];
    }

    /**
     * Execute the main thread that starts and terminates the benchmark threads
     *
     * @throws InterruptedException
     */
    private void execute(int milliseconds, boolean maint)
            throws InterruptedException {
		long startTime = System.currentTimeMillis();
        long count = fill(Parameters.range, Parameters.size);
		double initTime = ((double) (System.currentTimeMillis() - startTime)) / 1000.0;
		System.out.println("Initialization complete in (s) " + initTime + " operations " + count);

        System.out.println("Initialization complete. ");

        Thread.sleep(5000);
        startTime = System.currentTimeMillis();
        for (Thread thread : threads)
            thread.start();
        try {
            Thread.sleep(milliseconds);
        } finally {
            switch (benchType) {
                case OAKMAP:
                    for (ThreadLoopOak threadLoop : threadLoopsOak)
                        threadLoop.stopThread();
                    break;
            }
        }
        for (Thread thread : threads)
            thread.join();

        long endTime = System.currentTimeMillis();
        elapsedTime = ((double) (endTime - startTime)) / 1000.0;
    }

    public void clear() {
        switch (benchType) {
            case OAKMAP:
                oakBench.clear();
                break;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        boolean firstIteration = true;
        Test test = new Test(args);
        test.printParams();

        // warming up the JVM
        if (Parameters.warmUp != 0) {
            try {
                test.initThreads();
            } catch (Exception e) {
                System.err.println("Cannot launch operations.");
                e.printStackTrace();
            }
            test.execute(Parameters.warmUp * 1000, true);
            // give time to the JIT
            Thread.sleep(1000);
            if (Parameters.detailedStats)
                test.recordPreliminaryStats();
            test.clear();
            test.resetStats();
            System.out.println("Warmup complete");
        }

        // running the bench
        for (int i = 0; i < Parameters.iterations; i++) {
            if (!firstIteration) {
                // give time to the JIT
                Thread.sleep(1000);
                test.resetStats();
                test.clear();
            }
            try {
                test.initThreads();
            } catch (Exception e) {
                System.err.println("Cannot launch operations.");
                e.printStackTrace();
            }
            test.execute(Parameters.numMilliseconds, false);

            if (test.oakBench instanceof MaintenanceAlg) {
                ((MaintenanceAlg) test.oakBench).stopMaintenance();
                test.structMods += ((MaintenanceAlg) test.oakBench)
                        .getStructMods();
            }

            test.printBasicStats();
            if (Parameters.detailedStats) {
                test.printDetailedStats();
                ((OakMap) test.oakBench).printMemStats();
            }

            firstIteration = false;
            test.currentIteration++;
        }

        if (Parameters.iterations > 1) {
            test.printIterationStats();
        }
    }

    /* ---------------- Input/Output -------------- */

    /**
     * Parse the parameters on the command line
     */
    private void parseCommandLineParameters(String[] args) throws Exception {
        int argNumber = 0;

        while (argNumber < args.length) {
            String currentArg = args[argNumber++];

            try {
                if (currentArg.equals("--help") || currentArg.equals("-h")) {
                    printUsage();
                    System.exit(0);
                } else if (currentArg.equals("--verbose")
                        || currentArg.equals("-e")) {
                    Parameters.detailedStats = true;
                } else if (currentArg.equals("--change")
                        || currentArg.equals("-c")) {
                    Parameters.change = true;
                } else if (currentArg.equals("--buffer")) {
                    Parameters.zeroCopy = true;
                } else if (currentArg.equals("--inc")) {
                    Parameters.keyDistribution = Parameters.KeyDist.INCREASING;
                } else {
                    String optionValue = args[argNumber++];
                    if (currentArg.equals("--thread-nums")
                            || currentArg.equals("-t"))
                        Parameters.numThreads = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--duration")
                            || currentArg.equals("-d"))
                        Parameters.numMilliseconds = Integer
                                .parseInt(optionValue);
                    else if (currentArg.equals("--updates")
                            || currentArg.equals("-u"))
                        Parameters.numWrites = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--writeAll")
                            || currentArg.equals("-a"))
                        Parameters.numWriteAlls = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--snapshots")
                            || currentArg.equals("-s"))
                        Parameters.numSnapshots = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--size")
                            || currentArg.equals("-i"))
                        Parameters.size = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--range")
                            || currentArg.equals("-r"))
                        Parameters.range = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--Warmup")
                            || currentArg.equals("-W"))
                        Parameters.warmUp = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--benchmark")
                            || currentArg.equals("-b"))
                        Parameters.benchClassName = optionValue;
                    else if (currentArg.equals("--iterations")
                            || currentArg.equals("-n"))
                        Parameters.iterations = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--keySize")
                            || currentArg.equals("-k"))
                        Parameters.keySize = Integer.parseInt(optionValue);
                    else if (currentArg.equals("--valSize")
                            || currentArg.equals("-v"))
                        Parameters.valSize = Integer.parseInt(optionValue);
                }
            } catch (IndexOutOfBoundsException e) {
                System.err.println("Missing value after option: " + currentArg
                        + ". Ignoring...");
            } catch (NumberFormatException e) {
                System.err.println("Number expected after option:  "
                        + currentArg + ". Ignoring...");
            }
        }
        assert (Parameters.range >= Parameters.size);
        if (Parameters.range != 2 * Parameters.size)
            System.err
                    .println("Note that the value range is not twice "
                            + "the initial size, thus the size expectation varies at runtime.");
    }

    /**
     * Print a 80 character line filled with the same marker character
     *
     * @param ch the marker character
     */
    private void printLine(char ch) {
        StringBuffer line = new StringBuffer(79);
        for (int i = 0; i < 79; i++)
            line.append(ch);
        System.out.println(line);
    }

    /**
     * Print the header message on the standard output
     */
    private void printHeader() {
        String header = "Synchrobench-java\n"
                + "A benchmark-suite to evaluate synchronization techniques";
        printLine('-');
        System.out.println(header);
        printLine('-');
        System.out.println();
    }

    /**
     * Print the benchmark usage on the standard output
     */
    private void printUsage() {
        String syntax = "Usage:\n"
                + "java synchrobench.benchmark.Test [options] [-- stm-specific options]\n\n"
                + "Options:\n"
                + "\t-e            -- print detailed statistics (default: "
                + Parameters.detailedStats
                + ")\n"
                + "\t-t thread-num -- set the number of threads (default: "
                + Parameters.numThreads
                + ")\n"
                + "\t-d duration   -- set the length of the benchmark, in milliseconds (default: "
                + Parameters.numMilliseconds
                + ")\n"
                + "\t-u updates    -- set the number of threads (default: "
                + Parameters.numWrites
                + ")\n"
                + "\t-a writeAll   -- set the percentage of composite updates (default: "
                + Parameters.numWriteAlls
                + ")\n"
                + "\t-s snapshot   -- set the percentage of composite read-only operations (default: "
                + Parameters.numSnapshots
                + ")\n"
                + "\t-r range      -- set the element range (default: "
                + Parameters.range
                + ")\n"
                + "\t-b benchmark  -- set the benchmark (default: "
                + Parameters.benchClassName
                + ")\n"
                + "\t-i size       -- set the datastructure initial size (default: "
                + Parameters.size
                + ")\n"
                + "\t-n iterations -- set the bench iterations in the same JVM (default: "
                + Parameters.iterations
                + ")\n"
                + "\t-k keySize    -- set the size of the keys, in Bytes (default: "
                + Parameters.keySize
                + ")\n"
                + "\t-v valSize    -- set the size of the values, in Bytes (default: "
                + Parameters.valSize
                + ")\n"
                + "\t-c changeOp   -- change the operation (default: "
                + Parameters.change
                + ")\n"
                + "\t-W warmup     -- set the JVM warmup length, in seconds (default: "
                + Parameters.warmUp + ").";
        System.err.println(syntax);
    }

    /**
     * Print the parameters that have been given as an input to the benchmark
     */
    private void printParams() {
        String params = "Benchmark parameters" + "\n" + "--------------------"
                + "\n" + "  Detailed stats:          \t"
                + (Parameters.detailedStats ? "enabled" : "disabled")
                + "\n"
                + "  Number of threads:       \t"
                + Parameters.numThreads
                + "\n"
                + "  Length:                  \t"
                + Parameters.numMilliseconds
                + " ms\n"
                + "  Write ratio:             \t"
                + Parameters.numWrites
                + " %\n"
                + "  WriteAll ratio:          \t"
                + Parameters.numWriteAlls
                + " %\n"
                + "  Snapshot ratio:          \t"
                + Parameters.numSnapshots
                + " %\n"
                + "  Size:                    \t"
                + Parameters.size
                + " elts\n"
                + "  Range:                   \t"
                + Parameters.range
                + " elts\n"
                + "  WarmUp:                  \t"
                + Parameters.warmUp
                + " s\n"
                + "  Iterations:              \t"
                + Parameters.iterations
                + "\n"
                + "  Key size:              \t"
                + Parameters.keySize
                + " Bytes\n"
                + "  Val size:              \t"
                + Parameters.valSize
                + " Bytes\n"
                + "  Change:                \t"
                + Parameters.change
                + "\n"
                + "  Buffer view:            \t"
                + Parameters.zeroCopy
                + "\n"
                + "  Benchmark:               \t"
                + Parameters.benchClassName;
        System.out.println(params);
    }

    /**
     * Print the statistics on the standard output
     */
    private void printBasicStats() {
        for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
            switch (benchType) {
                case OAKMAP:
                    numAdd += threadLoopsOak[threadNum].numAdd;
                    numRemove += threadLoopsOak[threadNum].numRemove;
                    numAddAll += threadLoopsOak[threadNum].numAddAll;
                    numRemoveAll += threadLoopsOak[threadNum].numRemoveAll;
                    numSize += threadLoopsOak[threadNum].numSize;
                    numContains += threadLoopsOak[threadNum].numContains;
                    failures += threadLoopsOak[threadNum].failures;
                    total += threadLoopsOak[threadNum].total;
                    aborts += threadLoopsOak[threadNum].aborts;
                    getCount += threadLoopsOak[threadNum].getCount;
                    nodesTraversed += threadLoopsOak[threadNum].nodesTraversed;
                    structMods += threadLoopsOak[threadNum].structMods;
                    break;
            }
        }
        throughput[currentIteration] = ((double) total / elapsedTime);
        totalSize[currentIteration] = oakBench.size();
        printLine('-');
        System.out.println("Benchmark statistics");
        printLine('-');
        System.out.println("  Average traversal length: \t"
                + (double) nodesTraversed / (double) getCount);
        System.out.println("  Struct Modifications:     \t" + structMods);
        System.out.println("  Throughput (ops/s):       \t" + throughput[currentIteration]);
        System.out.println("  Elapsed time (s):         \t" + elapsedTime);
        System.out.println("  Operations:               \t" + total
                + "\t( 100 %)");
        System.out
                .println("    effective updates:     \t"
                        + (numAdd + numRemove + numAddAll + numRemoveAll)
                        + "\t( "
                        + formatDouble(((double) (numAdd + numRemove
                        + numAddAll + numRemoveAll) * 100)
                        / (double) total) + " %)");
        System.out.println("    |--add successful:     \t" + numAdd + "\t( "
                + formatDouble(((double) numAdd / (double) total) * 100)
                + " %)");
        System.out.println("    |--remove succ.:       \t" + numRemove + "\t( "
                + formatDouble(((double) numRemove / (double) total) * 100)
                + " %)");
        System.out.println("    |--addAll succ.:       \t" + numAddAll + "\t( "
                + formatDouble(((double) numAddAll / (double) total) * 100)
                + " %)");
        System.out.println("    |--removeAll succ.:    \t" + numRemoveAll
                + "\t( "
                + formatDouble(((double) numRemoveAll / (double) total) * 100)
                + " %)");
        System.out.println("    size successful:       \t" + numSize + "\t( "
                + formatDouble(((double) numSize / (double) total) * 100)
                + " %)");
        System.out.println("    contains succ.:        \t" + numContains
                + "\t( "
                + formatDouble(((double) numContains / (double) total) * 100)
                + " %)");
        System.out.println("    unsuccessful ops:      \t" + failures + "\t( "
                + formatDouble(((double) failures / (double) total) * 100)
                + " %)");
        switch (benchType) {
            case OAKMAP:
                System.out.println("  Final size:              \t" + totalSize[currentIteration]);
                if (Parameters.numWriteAlls == 0)
                    System.out.println("  Expected size:           \t" + (Parameters.size + numAdd - numRemove));
                break;
        }

        switch (benchType) {
            case OAKMAP:
                if (oakBench instanceof MaintenanceAlg) {
                    System.out.println("  #nodes (inc. deleted): \t"
                            + ((MaintenanceAlg) oakBench).numNodes());
                }
                break;
        }

    }

    /**
     * Detailed Warmup TM Statistics
     */
    private int numCommits = 0;
    private int numStarts = 0;
    private int numAborts = 0;

    private int numCommitsReadOnly = 0;
    private int numCommitsElastic = 0;
    private int numCommitsUpdate = 0;

    private int numAbortsBetweenSuccessiveReads = 0;
    private int numAbortsBetweenReadAndWrite = 0;
    private int numAbortsExtendOnRead = 0;
    private int numAbortsWriteAfterRead = 0;
    private int numAbortsLockedOnWrite = 0;
    private int numAbortsLockedBeforeRead = 0;
    private int numAbortsLockedBeforeElasticRead = 0;
    private int numAbortsLockedOnRead = 0;
    private int numAbortsInvalidCommit = 0;
    private int numAbortsInvalidSnapshot = 0;

    private double readSetSizeSum = 0.0;
    private double writeSetSizeSum = 0.0;
    private int statSize = 0;
    private int txDurationSum = 0;
    private int elasticReads = 0;
    private int readsInROPrefix = 0;

    /**
     * This method is called between two runs of the benchmark within the same
     * JVM to enable its warmup
     */
    public void resetStats() {

        for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
            switch (benchType) {
                case OAKMAP:
                    threadLoopsOak[threadNum].numAdd = 0;
                    threadLoopsOak[threadNum].numRemove = 0;
                    threadLoopsOak[threadNum].numAddAll = 0;
                    threadLoopsOak[threadNum].numRemoveAll = 0;
                    threadLoopsOak[threadNum].numSize = 0;
                    threadLoopsOak[threadNum].numContains = 0;
                    threadLoopsOak[threadNum].failures = 0;
                    threadLoopsOak[threadNum].total = 0;
                    threadLoopsOak[threadNum].aborts = 0;
                    threadLoopsOak[threadNum].nodesTraversed = 0;
                    threadLoopsOak[threadNum].getCount = 0;
                    threadLoopsOak[threadNum].structMods = 0;
                    break;
            }

        }
        numAdd = 0;
        numRemove = 0;
        numAddAll = 0;
        numRemoveAll = 0;
        numSize = 0;
        numContains = 0;
        failures = 0;
        total = 0;
        aborts = 0;
        nodesTraversed = 0;
        getCount = 0;
        structMods = 0;

        numCommits = 0;
        numStarts = 0;
        numAborts = 0;

        numCommitsReadOnly = 0;
        numCommitsElastic = 0;
        numCommitsUpdate = 0;

        numAbortsBetweenSuccessiveReads = 0;
        numAbortsBetweenReadAndWrite = 0;
        numAbortsExtendOnRead = 0;
        numAbortsWriteAfterRead = 0;
        numAbortsLockedOnWrite = 0;
        numAbortsLockedBeforeRead = 0;
        numAbortsLockedBeforeElasticRead = 0;
        numAbortsLockedOnRead = 0;
        numAbortsInvalidCommit = 0;
        numAbortsInvalidSnapshot = 0;

        readSetSizeSum = 0.0;
        writeSetSizeSum = 0.0;
        statSize = 0;
        txDurationSum = 0;
        elasticReads = 0;
        readsInROPrefix = 0;
    }

    public void recordPreliminaryStats() {
        numAborts = Statistics.getTotalAborts();
        numCommits = Statistics.getTotalCommits();
        numCommitsReadOnly = Statistics.getNumCommitsReadOnly();
        numCommitsElastic = Statistics.getNumCommitsElastic();
        numCommitsUpdate = Statistics.getNumCommitsUpdate();
        numStarts = Statistics.getTotalStarts();
        numAbortsBetweenSuccessiveReads = Statistics
                .getNumAbortsBetweenSuccessiveReads();
        numAbortsBetweenReadAndWrite = Statistics
                .getNumAbortsBetweenReadAndWrite();
        numAbortsExtendOnRead = Statistics.getNumAbortsExtendOnRead();
        numAbortsWriteAfterRead = Statistics.getNumAbortsWriteAfterRead();
        numAbortsLockedOnWrite = Statistics.getNumAbortsLockedOnWrite();
        numAbortsLockedBeforeRead = Statistics.getNumAbortsLockedBeforeRead();
        numAbortsLockedBeforeElasticRead = Statistics
                .getNumAbortsLockedBeforeElasticRead();
        numAbortsLockedOnRead = Statistics.getNumAbortsLockedOnRead();
        numAbortsInvalidCommit = Statistics.getNumAbortsInvalidCommit();
        numAbortsInvalidSnapshot = Statistics.getNumAbortsInvalidSnapshot();
        readSetSizeSum = Statistics.getSumReadSetSize();
        writeSetSizeSum = Statistics.getSumWriteSetSize();
        ;
        statSize = Statistics.getStatSize();
        txDurationSum = Statistics.getSumCommitingTxTime();
        elasticReads = Statistics.getTotalElasticReads();
        readsInROPrefix = Statistics.getTotalReadsInROPrefix();
    }

    /**
     * Print the detailed statistics on the standard output
     */
    private void printDetailedStats() {

        numCommits = Statistics.getTotalCommits() - numCommits;
        numStarts = Statistics.getTotalStarts() - numStarts;
        numAborts = Statistics.getTotalAborts() - numAborts;

        numCommitsReadOnly = Statistics.getNumCommitsReadOnly()
                - numCommitsReadOnly;
        numCommitsElastic = Statistics.getNumCommitsElastic()
                - numCommitsElastic;
        numCommitsUpdate = Statistics.getNumCommitsUpdate() - numCommitsUpdate;

        numAbortsBetweenSuccessiveReads = Statistics
                .getNumAbortsBetweenSuccessiveReads()
                - numAbortsBetweenSuccessiveReads;
        numAbortsBetweenReadAndWrite = Statistics
                .getNumAbortsBetweenReadAndWrite()
                - numAbortsBetweenReadAndWrite;
        numAbortsExtendOnRead = Statistics.getNumAbortsExtendOnRead()
                - numAbortsExtendOnRead;
        numAbortsWriteAfterRead = Statistics.getNumAbortsWriteAfterRead()
                - numAbortsWriteAfterRead;
        numAbortsLockedOnWrite = Statistics.getNumAbortsLockedOnWrite()
                - numAbortsLockedOnWrite;
        numAbortsLockedBeforeRead = Statistics.getNumAbortsLockedBeforeRead()
                - numAbortsLockedBeforeRead;
        numAbortsLockedBeforeElasticRead = Statistics
                .getNumAbortsLockedBeforeElasticRead()
                - numAbortsLockedBeforeElasticRead;
        numAbortsLockedOnRead = Statistics.getNumAbortsLockedOnRead()
                - numAbortsLockedOnRead;
        numAbortsInvalidCommit = Statistics.getNumAbortsInvalidCommit()
                - numAbortsInvalidCommit;
        numAbortsInvalidSnapshot = Statistics.getNumAbortsInvalidSnapshot()
                - numAbortsInvalidSnapshot;

        assert (numAborts == (numAbortsBetweenSuccessiveReads
                + numAbortsBetweenReadAndWrite + numAbortsExtendOnRead
                + numAbortsWriteAfterRead + numAbortsLockedOnWrite
                + numAbortsLockedBeforeRead + numAbortsLockedBeforeElasticRead
                + numAbortsLockedOnRead + numAbortsInvalidCommit + numAbortsInvalidSnapshot));

        assert (numStarts - numAborts) == numCommits;

        readSetSizeSum = Statistics.getSumReadSetSize() - readSetSizeSum;
        writeSetSizeSum = Statistics.getSumWriteSetSize() - writeSetSizeSum;
        statSize = Statistics.getStatSize() - statSize;
        txDurationSum = Statistics.getSumCommitingTxTime() - txDurationSum;

        printLine('-');
        System.out.println("TM statistics");
        printLine('-');

        System.out.println("  Commits:                  \t" + numCommits);
        System.out
                .println("  |--regular read only  (%) \t"
                        + numCommitsReadOnly
                        + "\t( "
                        + formatDouble(((double) numCommitsReadOnly / (double) numCommits) * 100)
                        + " %)");
        System.out
                .println("  |--elastic (%)            \t"
                        + numCommitsElastic
                        + "\t( "
                        + formatDouble(((double) numCommitsElastic / (double) numCommits) * 100)
                        + " %)");
        System.out
                .println("  |--regular update (%)     \t"
                        + numCommitsUpdate
                        + "\t( "
                        + formatDouble(((double) numCommitsUpdate / (double) numCommits) * 100)
                        + " %)");
        System.out.println("  Starts:                   \t" + numStarts);
        System.out.println("  Aborts:                   \t" + numAborts
                + "\t( 100 %)");
        System.out
                .println("  |--between succ. reads:   \t"
                        + (numAbortsBetweenSuccessiveReads)
                        + "\t( "
                        + formatDouble(((double) (numAbortsBetweenSuccessiveReads) * 100)
                        / (double) numAborts) + " %)");
        System.out
                .println("  |--between read & write:  \t"
                        + numAbortsBetweenReadAndWrite
                        + "\t( "
                        + formatDouble(((double) numAbortsBetweenReadAndWrite / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--extend upon read:      \t"
                        + numAbortsExtendOnRead
                        + "\t( "
                        + formatDouble(((double) numAbortsExtendOnRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--write after read:      \t"
                        + numAbortsWriteAfterRead
                        + "\t( "
                        + formatDouble(((double) numAbortsWriteAfterRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked on write:       \t"
                        + numAbortsLockedOnWrite
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedOnWrite / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked before read:    \t"
                        + numAbortsLockedBeforeRead
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedBeforeRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked before eread:   \t"
                        + numAbortsLockedBeforeElasticRead
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedBeforeElasticRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked on read:        \t"
                        + numAbortsLockedOnRead
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedOnRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--invalid commit:        \t"
                        + numAbortsInvalidCommit
                        + "\t( "
                        + formatDouble(((double) numAbortsInvalidCommit / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--invalid snapshot:      \t"
                        + numAbortsInvalidSnapshot
                        + "\t( "
                        + formatDouble(((double) numAbortsInvalidSnapshot / (double) numAborts) * 100)
                        + " %)");
        System.out.println("  Read set size on avg.:    \t"
                + formatDouble(readSetSizeSum / statSize));
        System.out.println("  Write set size on avg.:   \t"
                + formatDouble(writeSetSizeSum / statSize));
        System.out.println("  Tx time-to-commit on avg.:\t"
                + formatDouble((double) txDurationSum / numCommits)
                + " microsec");
        System.out.println("  Number of elastic reads       " + elasticReads);
        System.out
                .println("  Number of reads in RO prefix  " + readsInROPrefix);
    }

    /**
     * Print the iteration statistics on the standard output
     */
    private void printIterationStats() {
        printLine('-');
        System.out.println("Iteration statistics");
        printLine('-');

        int n = Parameters.iterations;
        System.out.println("  Iterations:                 \t" + n);
        double sum = 0;
        int sizeSum = 0;
        for (int i = 0; i < n; i++) {
            sum += ((throughput[i] / 1024) / 1024);
            sizeSum += totalSize[i];
        }
        System.out.println("  Total throughput (mebiops/s): " + sum);
        double mean = sum / n;
        double meanSize = (double) sizeSum / n;
        System.out.println("  |--Mean:                    \t" + mean);
        System.out.println("  |--Mean Total Size:         \t" + meanSize);
        double temp = 0;
        for (int i = 0; i < n; i++) {
            double diff = ((throughput[i] / 1024) / 1024) - mean;
            temp += diff * diff;
        }
        double var = temp / n;
        System.out.println("  |--Variance:                \t" + var);
        double stdevp = java.lang.Math.sqrt(var);
        System.out.println("  |--Standard deviation pop:  \t" + stdevp);
        double sterr = stdevp / java.lang.Math.sqrt(n);
        System.out.println("  |--Standard error:          \t" + sterr);
        System.out.println("  |--Margin of error (95% CL):\t" + (sterr * 1.96));
    }

    private static String formatDouble(double result) {
        Formatter formatter = new Formatter(Locale.US);
        return formatter.format("%.2f", result).out().toString();
    }
}
