/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.test_utils.ExecutorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;


@RunWith(Parameterized.class)
public class MultiThreadComputeTest {
    private static final int NUM_THREADS = 31;
    // prolong due to parameterization, and depending on the hash function
    private static final long TIME_LIMIT_IN_SECONDS = 120;
    private static final int MAX_ITEMS_PER_CHUNK = 256; // was 1024

    private ConcurrentZCMap<Integer, Integer> oak;
    private ExecutorUtils<Void> executor;

    private CountDownLatch latch;
    private Consumer<OakScopedWriteBuffer> computer;
    private Consumer<OakScopedWriteBuffer> emptyComputer;
    

    private Supplier<ConcurrentZCMap> builder;


    public MultiThreadComputeTest(Supplier<ConcurrentZCMap> supplier) {
        this.builder = supplier;

    }

    @Parameterized.Parameters
    public static Collection parameters() {

        Supplier<ConcurrentZCMap> s1 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                    .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
            return builder.buildOrderedMap();
        };
        Supplier<ConcurrentZCMap> s2 = () -> {
            OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(MAX_ITEMS_PER_CHUNK);
            return builder.buildHashMap();
        };
        return Arrays.asList(new Object[][] {
                { s1 }
                //{ s2 } 
                //TODO have problem in hash with asserts, do we want to remove asserts?
        });
    }





    @Before
    public void init() {

        oak = builder.get();
        latch = new CountDownLatch(1);
        executor = new ExecutorUtils<>(NUM_THREADS);
        computer = oakWBuffer -> {
            if (oakWBuffer.getInt(0) == 0) {
                oakWBuffer.putInt(0, 1);
            }
        };
        emptyComputer = oakWBuffer -> {
        };
    }

    @After
    public void finish() {
        executor.shutdownNow();
        oak.close();
        BlocksPool.clear();
    }

    class RunThreads implements Callable<Void> {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Void call() throws InterruptedException {

            // make each thread to start from different start points so there is no simultaneous
            // insertion of the same keys, as this is currently not supported for OakHash
            // TODO: change the contention back
            int threadId = ThreadIndexCalculator.newInstance().getIndex();
            int int2start = (threadId % NUM_THREADS) * (MAX_ITEMS_PER_CHUNK * 20);
            
            int startRegion1 = int2start;
            int endRegion1   = int2start + MAX_ITEMS_PER_CHUNK;

            int startRegion2 = int2start + MAX_ITEMS_PER_CHUNK;
            int endRegion2   = int2start + (2 * MAX_ITEMS_PER_CHUNK);

            int startRegion3 = int2start + ( 2 * MAX_ITEMS_PER_CHUNK);
            int endRegion3   = int2start + (3 * MAX_ITEMS_PER_CHUNK);
            
            int startRegion4 = int2start + ( 3 * MAX_ITEMS_PER_CHUNK);
            int endRegion4   = int2start + (4 * MAX_ITEMS_PER_CHUNK);

            int startRegion5 = int2start + ( 4 * MAX_ITEMS_PER_CHUNK);            
            int endRegion6   = int2start + (6 * MAX_ITEMS_PER_CHUNK);
            
                        
            latch.await();

            boolean result = false;

            for (Integer i = startRegion1; i < endRegion4; i++) {                
                if (i == int2start) {
                    result = oak.zc().putIfAbsent(i, i);
                    if (!result) {
                        System.out.println(
                                "Key " + i + " existed. Thread ID " + threadId + ". Weird....");
                    }
                } else {
                    oak.zc().putIfAbsent(i, i);
                }
            }
              //in map all of this if ( i == int2start) can be removed, 
              //and replaced with just oak.zc().putifAbsent(i,i). 
              //There was a rare case where thread with startRegion1 =0,
              //tries to insert 0, but already exist since line 168.
              //In hash there is cases where start_region != 0 and we see existed value)

            Integer value = oak.get(startRegion1);
            if (value == null) {
                oak.get(startRegion1);
                System.out.println("Got a null from " + int2start + "!");
            }
            Assert.assertNotNull(value);

            for (Integer i = startRegion1; i < endRegion4; i++) {
                assert !oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }

            value = oak.get(startRegion1);
            if (value == null) {
                value = oak.get(startRegion1);
                System.out.println("Got a null from " + int2start + "!");
            }
            Assert.assertNotNull(value);


            for (int i = startRegion4 ; i < endRegion4; i++) {
                oak.zc().remove(i);
            }

            value = oak.get(startRegion1);
            if (value == null) {
                value = oak.get(startRegion1);
                System.out.println("Got a null!");
            }
            Assert.assertNotNull(value);

            oak.zc().put(1, 2);
            oak.zc().put(0, 1);

            for (int i = startRegion1; i < endRegion3; i++) {
                assert oak.zc().computeIfPresent(i, computer);
            }
            
            for (int i = startRegion4; i < endRegion4; i++) {
                assert !oak.zc().computeIfPresent(i, computer);
            }


            value = oak.get(startRegion1);
            if (value == null) {
                value = oak.get(startRegion1);
                System.out.println("Got a null from " + int2start + "!");
            }
            Assert.assertNotNull(value);

            for (int i = startRegion1; i < endRegion1; i++) {
                oak.zc().putIfAbsentComputeIfPresent(i, i, computer);
            }

            value = oak.get(startRegion1);
            if (value == null) {
                System.out.println("Got a null!");
            }
            Assert.assertNotNull(value);
            if (int2start == 0) {
                Assert.assertEquals((Integer) 1, value);
            } else if (int2start == 1) {
                Assert.assertEquals((Integer) 2, value);
            } else {
                Assert.assertEquals((Integer) startRegion1, value);
            }

            for (int i = startRegion2; i < endRegion2; i++) {
                assert oak.zc().remove(i);
            }

            for (int i = startRegion5; i < endRegion6; i++) {
                assert oak.zc().putIfAbsent(i, i);
            }

            for (int i = startRegion5; i < endRegion6; i++) {
                assert oak.zc().remove(i);
            }

            for (int i = startRegion4; i < endRegion4; i++) {
                assert oak.zc().putIfAbsent(i, i);
            }

            for (int i = startRegion4; i < endRegion4; i++) {
                assert !oak.zc().putIfAbsentComputeIfPresent(i, i, emptyComputer);
            }
            

            for (int i = startRegion4; i < endRegion4; i++) {
                assert oak.zc().remove(i);
            }

            for (int i = startRegion3; i < endRegion3; i++) {
                oak.zc().put(i, i);
            }

            for (int i = startRegion4; i < endRegion4; i++) {
                assert !oak.zc().remove(i);
            }

            for (int i = startRegion2; i < endRegion2; i++) {
                assert !oak.zc().remove(i);
            }

            for (int i = startRegion5; i < endRegion6; i++) {
                assert oak.zc().putIfAbsent(i, i);
            }

            return null;
        }
    }

    @Test
    public void testThreadsCompute() throws ExecutorUtils.ExecutionError, InterruptedException {
        executor.submitTasks(NUM_THREADS, i -> new MultiThreadComputeTest.RunThreads(latch));
        Thread.sleep(1000); //allow things to cookup!
        latch.countDown();
        executor.shutdown(TIME_LIMIT_IN_SECONDS);
        
        //we always will have int2start 0
        int startRegion1 = 0;
        int endRegion1   = startRegion1 + MAX_ITEMS_PER_CHUNK;
        
        int startRegion2 = endRegion1;
        int endRegion2   = startRegion2 + MAX_ITEMS_PER_CHUNK;

        int startRegion3 = endRegion2;
        int endRegion3   = startRegion3 + MAX_ITEMS_PER_CHUNK;
        
        int startRegion4 = endRegion3;
        int endRegion4   = startRegion4 + MAX_ITEMS_PER_CHUNK;
        
        int startRegion5 = endRegion4;
        int endRegion6   = startRegion5 + (2 * MAX_ITEMS_PER_CHUNK);

        
        for (Integer i = startRegion1; i < endRegion1; i++) { 
            Integer value = oak.get(i);

            if (value == null) {
                System.out.println("Got a null!");
            }
            Assert.assertNotNull(value);
            if (i == 0) {
                Assert.assertEquals((Integer) 1, value);
                continue;
            }
            if (i == 1) {
                Assert.assertEquals((Integer) 2, value);
                continue;
            }
            Assert.assertEquals(i, value);
        }
        
        for (int    i = startRegion2; i < endRegion2; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = startRegion3; i < endRegion3; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }
        for (int    i = startRegion4; i < endRegion4; i++) {
            Integer value = oak.get(i);
            Assert.assertNull(value);
        }

        for (Integer i = startRegion5; i < endRegion6; i++) {
            Integer value = oak.get(i);
            Assert.assertEquals(i, value);
        }

    }


}
