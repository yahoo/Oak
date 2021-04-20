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
            tasks.add(executor.submit(taskGenerator.apply(i)));
        }
    }

    public void shutdown(long timeLimitInSeconds) throws ExecutionException, InterruptedException, TimeoutException {
        shutdownTaskPool(executor, tasks, TimeUnit.MILLISECONDS.convert(timeLimitInSeconds, TimeUnit.SECONDS));
    }

    public void shutdownNow() {
        executor.shutdownNow();
    }

    private static long timeDiffMillis(Instant start) {
        return Duration.between(start, Instant.now()).toMillis();
    }

    /**
     * Closes the Executor thread pool and wait for the tasks to complete for a specified time limit
     *
     * @param executor      the thread pool to be closed
     * @param pendingTasks  list of tasks that need to end before the time limit
     * @param timeLimitInMs the time limit of the tasks to be done in milliseconds
     */
    public static void shutdownTaskPool(ExecutorService executor, List<Future<?>> pendingTasks, long timeLimitInMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        try {
            executor.shutdown();
            Instant startingTime = Instant.now();

            while (!pendingTasks.isEmpty() && timeDiffMillis(startingTime) <= timeLimitInMs) {
                Iterator<Future<?>> it = pendingTasks.iterator();
                while (it.hasNext()) {
                    Future<?> task = it.next();
                    long timeToWait = Math.max(1, timeLimitInMs - timeDiffMillis(startingTime));

                    // task.get() will throw an error if:
                    //  * the task had an exception
                    //  * if the thread got interrupted
                    //  * the timeToWait passed
                    task.get(timeToWait, TimeUnit.MILLISECONDS);

                    // If no exception was thrown (i.e., the task was successful and finished in time),
                    // we can remove it from the pendingTasks list.
                    it.remove();
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
