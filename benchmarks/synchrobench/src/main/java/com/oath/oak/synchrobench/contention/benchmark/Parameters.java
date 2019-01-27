package com.oath.oak.synchrobench.contention.benchmark;

/**
 * Parameters of the Java version of the 
 * Synchrobench benchmark.
 *
 * @author Vincent Gramoli
 */
public class Parameters {
    
    public static int numThreads = 1,
    	numMilliseconds = 5000,
    	numWrites = 0,
    	numWriteAlls = 0,
    	numSnapshots = 0,
    	range = 2048,
		size = 1024,
		warmUp = 5,
    	iterations = 1,
		keySize = 4,
        valSize = 4;
    
    public static boolean detailedStats = false;
	static boolean change = false;

    static String benchClassName = "skiplists.lockfree.NonBlockingFriendlySkipListMap";
}
