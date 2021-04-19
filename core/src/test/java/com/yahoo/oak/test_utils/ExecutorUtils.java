/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.test_utils;


import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;


public class ExecutorUtils {

    final ExecutorService executor;
    final List<Future<?>> tasks = new ArrayList<>();

    public ExecutorUtils(int numThreads) {
        this.executor = Executors.newFixedThreadPool(numThreads);
    }

    public <T> void submitTasks(int numTasks, Function<Integer, Callable<T>> taskGenerator) {
        for (int i = 0; i < numTasks; i++) {
            tasks.add(executor.submit(taskGenerator.apply(i))) ;
        }
    }

    public void shutdown(long timeLimitInSeconds) throws ExecutionException, InterruptedException, TimeoutException {
        shutdownTaskPool(executor, tasks, TimeUnit.MILLISECONDS.convert(timeLimitInSeconds, TimeUnit.SECONDS));
    }

    public void shutdownNow() {
        executor.shutdownNow();
    }

    /**
     * this function close the Executor thread pool  also wait for given tasks to complete upto given time limit
     * @param executor the thread pool to be closed
     * @param pendingTasks list of tasks that need to end before the time limit
     * @param timeLimitInMs the time limit of the tasks to be done in milliseconds
     */
    public static void shutdownTaskPool(ExecutorService executor, List<Future<?>>pendingTasks, long timeLimitInMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        try {
            executor.shutdown();
            Instant startingTime = Instant.now();
            Instant currentTime;
            do {
                Iterator<Future<?>> it = pendingTasks.iterator();
                while (it.hasNext()) {
                    Future<?> task = it.next();
                    currentTime = Instant.now();
                    long timeToWait = Math.max(10,
                            timeLimitInMs - Duration.between(startingTime, currentTime).toMillis());
                    //task.get will throw error if the task had an exception or if the timelimit passed
                    // or if the thread got interrupted exception
                    task.get(timeToWait, TimeUnit.MILLISECONDS);
                    it.remove();
                }
                currentTime = Instant.now();
            } while (!pendingTasks.isEmpty() &&
                    Duration.between(startingTime, currentTime).toMillis() <= timeLimitInMs);
        }  finally {
            executor.shutdownNow();
        }
    }
}
