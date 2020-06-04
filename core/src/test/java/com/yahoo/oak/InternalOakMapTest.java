/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class InternalOakMapTest {

    private InternalOakMap<Integer, Integer> testMap;

    private static final long OPERATION_DELAY = 100;
    private static final long LONG_TRANSFORMATION_DELAY = 1000;

    @Before
    public void setUp() {
        NovaManager memoryManager = new NovaManager(new NativeMemoryAllocator(128));
        int chunkMaxItems = 100;

        testMap = new InternalOakMap<>(Integer.MIN_VALUE, OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER, OakCommonBuildersFactory.DEFAULT_INT_COMPARATOR,
                memoryManager, chunkMaxItems, new ValueUtilsImpl());
    }


    private static Integer slowDeserialize(OakScopedReadBuffer bb) {
        try {
            Thread.sleep(LONG_TRANSFORMATION_DELAY);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER.deserialize(bb);
    }

    private void runThreads(List<Thread> threadList) throws InterruptedException {
        for (Thread thread : threadList) {
            thread.start();
            Thread.sleep(OPERATION_DELAY);
        }

        for (Thread thread : threadList) {
            thread.join();
        }
    }


    @Test
    public void concurrentPuts() throws InterruptedException {
        Integer k = 1;
        Integer v1 = 1;
        Integer v2 = 2;
        Integer v3 = 3;

        final Integer[] results = new Integer[3];

        List<Thread> threadList = new ArrayList<>(results.length);
        threadList.add(new Thread(() -> results[0] = testMap.put(k, v1,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize)));
        threadList.add(new Thread(() -> results[1] = testMap.put(k, v2, InternalOakMapTest::slowDeserialize)));
        threadList.add(new Thread(() -> results[2] = testMap.put(k, v3,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize)));

        runThreads(threadList);

        Assert.assertNull(results[0]);
        Assert.assertNotEquals(results[1], results[2]);
    }

    @Test
    public void concurrentPutAndRemove() throws InterruptedException {
        Integer k = 1;
        Integer v1 = 1;
        Integer v2 = 2;

        final Integer[] results = new Integer[3];

        List<Thread> threadList = new ArrayList<>(results.length);
        threadList.add(new Thread(() -> results[0] = testMap.put(k, v1,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize)));
        threadList.add(new Thread(() -> results[1] =
                (Integer) testMap.remove(k, null, InternalOakMapTest::slowDeserialize).value));
        threadList.add(new Thread(() -> results[2] = testMap.put(k, v2,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize)));

        runThreads(threadList);

        Assert.assertNull(results[0]);
        Assert.assertNotEquals(results[1], results[2]);
    }

    @Test
    public void concurrentRemove() throws InterruptedException {
        Integer k = 1;
        Integer v1 = 1;

        final Integer[] results = new Integer[2];

        testMap.put(k, v1, OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);

        List<Thread> threadList = new ArrayList<>(results.length);
        threadList.add(new Thread(() -> results[0] =
                (Integer) testMap.remove(k, null, InternalOakMapTest::slowDeserialize).value));
        threadList.add(new Thread(() -> results[1] =
                (Integer) testMap.remove(k, null, OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize).value));

        runThreads(threadList);

        Assert.assertNotEquals(results[0], results[1]);
    }
}
