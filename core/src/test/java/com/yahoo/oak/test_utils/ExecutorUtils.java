/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.test_utils;


import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class ExecutorUtils {

    public  static  void shutdownTaskPool(ExecutorService executor, List<Future<?>>pendingTasks, long timeLimitInMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        /***
         * this function close the Executor thread pool  also wait for given tasks to complete upto given time limit
         * @param executor the thread pool to be closed
         * @param pendingTasks list of tasks that are ruin
         * @param timeLimitInMs the time limit of the tasks to be done in milliseconds
         */
        executor.shutdown();
        Instant startingTime = Instant.now();
        Instant currentTime = null;
        do {
            Iterator<Future<?>> it = pendingTasks.iterator();
            while (it.hasNext()) {
                Future<?> task=it.next();
                currentTime = Instant.now();
                long timeToWait = Math.max(10,
                        timeLimitInMs-Duration.between(startingTime, currentTime).toMillis());
                task.get(timeToWait, TimeUnit.MILLISECONDS);
                it.remove();
            }
            currentTime=Instant.now();
        }while (!pendingTasks.isEmpty()&&
                Duration.between(startingTime ,  currentTime ).toMillis()<=timeLimitInMs);
    //    try {
    //        long waiting_time=
    //        pending_tasks.forEach(
    //                (future -> future.get())
    //        );
    //        if (!executor.awaitTermination(time_limit_in_ms, TimeUnit.MILLISECONDS)) {
    //            executor.shutdownNow();
    //            Assert.fail("should have done all the tasks in time");
    //        }
    //    } catch (InterruptedException e) {
    //        executor.shutdownNow();
    //        Assert.fail("failed to run all the tasks in the executor service");
    //    }
        return ;
    }
}
