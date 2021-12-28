/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class InternalOakHashTest {

    private InternalOakHash<Integer, Integer> testMap;

    private static final long OPERATION_DELAY = 10;
    private static final long LONG_TRANSFORMATION_DELAY = 100;
    private static final int ONE_THREAD_OPERATIONS_NUM = 100;
    // Chunk is going to have 2^9=512 enries. As integer's hashCode()
    // is a pure function, make sure there is no more than 512 keys per chunk
    private static final int BITS_TO_DEFINE_CHUNK_SIZE = 9;

    @Before
    public void setUp() {
        NativeMemoryAllocator ma = new NativeMemoryAllocator(1024);
        SyncRecycleMemoryManager memoryManager = new SyncRecycleMemoryManager(ma);
        // 8 MSBs taken from keyHash for first level hash array index,
        // meaning in the first level hash array there are 2^8=256 chunks
        int firstLevelBitSize = 8;
        // 8 LSBs taken from keyHash for a chunk index, meaning in chunk there are 2^8=256 entries

        testMap = new InternalOakHash<>(new OakSharedConfig<>(
                ma, memoryManager, memoryManager,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER,
                OakCommonBuildersFactory.DEFAULT_INT_COMPARATOR
        ), firstLevelBitSize, BITS_TO_DEFINE_CHUNK_SIZE);
    }

    @After
    public void tearDown() {
        testMap.close();
        BlocksPool.clear();
    }

    @After
    public void tearDown() {
        testMap.close();
        BlocksPool.clear();
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
    public void concurrentSimplePuts() throws InterruptedException {
        Integer k1 = 1;
        Integer k2 = 2;
        Integer k3 = 3;
        Integer v1 = 10;
        Integer v2 = 20;
        Integer v3 = 30;
        Integer v11 = 100;

        final Integer[] results = new Integer[3];

        List<Thread> threadList = new ArrayList<>(results.length);

        threadList.add(new Thread(() -> results[0] = testMap.put(k1, v1,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize)));

        threadList.add(new Thread(() -> results[1] = testMap.put(k2, v2, InternalOakHashTest::slowDeserialize)));

        threadList.add(new Thread(() -> results[2] = testMap.put(k3, v3,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize)));

        runThreads(threadList);

        Assert.assertNull(results[0]);
        Assert.assertNull(results[1]);
        Assert.assertNull(results[2]);

        OakUnscopedBuffer v1buffer = testMap.get(k1);
        Integer v1returned = v1buffer.transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
        Assert.assertEquals(v1, v1returned);

        OakUnscopedBuffer v2buffer = testMap.get(k2);
        Integer v2returned = v2buffer.transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
        Assert.assertEquals(v2, v2returned);

        OakUnscopedBuffer v3buffer = testMap.get(k3);
        Integer v3returned = v3buffer.transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
        Assert.assertEquals(v3, v3returned);

        // update first key again
        v1returned = testMap.put(k1, v11, OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
        Assert.assertEquals(v1, v1returned); // previous value needs to be returned
        v1buffer = testMap.get(k1);
        v1returned = v1buffer.transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
        Assert.assertEquals(v11, v1returned);

    }


    @Test
    public void concurrentPuts() throws InterruptedException {
        final Integer k1 = 1;   // keys k1/2/3 are going to the same chunk
        final Integer k2 = 100;
        final Integer k3 = 10000;
        List<Thread> threadList = new ArrayList<>(3);

        Thread firstThread = new Thread(() -> {
            for (int i = 0; i < ONE_THREAD_OPERATIONS_NUM; i++) {
                Integer currentKey = k1 + (i * 10);
                Integer v = currentKey * 10;
                Integer result = testMap.put(currentKey, v,
                    OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
                Assert.assertNull(result);
            }

            for (int i = 0; i < ONE_THREAD_OPERATIONS_NUM; i++) {
                Integer currentKey = k1 + (i * 10);
                OakUnscopedBuffer vBuffer = testMap.get(currentKey);
                Integer vReturned = vBuffer.transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
                Integer v = currentKey * 10;
                Assert.assertEquals(v, vReturned);
            }
        });

        Thread secondThread = new Thread(() -> {
            for (int i = 0; i < ONE_THREAD_OPERATIONS_NUM; i++) {
                Integer currentKey = k2 + (i * 10);
                Integer v = currentKey * 10;
                Integer result = testMap.put(currentKey, v,
                    InternalOakHashTest::slowDeserialize);
                Assert.assertNull(result);
            }
        });

        Thread thirdThread = new Thread(() -> {
            for (int i = 0; i < ONE_THREAD_OPERATIONS_NUM; i++) {
                Integer currentKey = k3 + (i * 10);
                Integer v = currentKey * 10;
                Integer result = testMap.put(currentKey, v,
                    OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
                Assert.assertNull(result);
            }
        });

        threadList.add(firstThread);
        threadList.add(secondThread);
        threadList.add(thirdThread);
        runThreads(threadList);

        Assert.assertEquals(ONE_THREAD_OPERATIONS_NUM * 3, testMap.size.get());

        // gets of the first thread insertions are tested concurrently within the thread
        // here test second and third
        for (int i = 0; i < ONE_THREAD_OPERATIONS_NUM; i++) {
            Integer currentKey = k2 + (i * 10);
            OakUnscopedBuffer vBuffer = testMap.get(currentKey);
            Integer vReturned = vBuffer.transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Integer v = currentKey * 10;
            Assert.assertEquals(v, vReturned);
        }

        // here test the non-ZC variation of the get, including simple key transformation
        for (int i = 0; i < ONE_THREAD_OPERATIONS_NUM; i++) {
            Integer currentKey = k3 + (i * 10);
            Integer vReturned = testMap.getValueTransformation(currentKey,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Integer v = currentKey * 10;
            Assert.assertEquals(v, vReturned);
            Integer kReturned = testMap.getKeyTransformation(currentKey,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Assert.assertEquals(currentKey, kReturned);
        }

        // update same keys from the first thread again
        for (int i = 0; i < ONE_THREAD_OPERATIONS_NUM; i++) {
            Integer currentKey = k1 + (i * 10);
            Integer v1new = currentKey * 100;
            Integer v1old = currentKey * 10;
            Integer v1returned = testMap.put(currentKey, v1new,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Assert.assertEquals(v1old, v1returned); // previous value needs to be returned
            OakUnscopedBuffer v1buffer = testMap.get(currentKey);
            v1returned = v1buffer.transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Assert.assertEquals(v1new, v1returned);
        }

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
                (Integer) testMap.remove(k, null, InternalOakHashTest::slowDeserialize).value));

        threadList.add(new Thread(() -> results[2] = testMap.put(k, v2,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize)));

        runThreads(threadList);

        if (results[0] == null) {
            // first put was successfull
            Assert.assertNotEquals(results[1], results[2]);
        } else {
            // last put was successful
            Assert.assertNotEquals(results[1], results[0]);
        }
    }

    @Test
    public void concurrentRemove() throws InterruptedException {
        Integer k = 1;
        Integer v1 = 1;

        final Integer[] results = new Integer[2];

        testMap.put(k, v1, OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);

        List<Thread> threadList = new ArrayList<>(results.length);
        threadList.add(new Thread(() -> results[0] =
                (Integer) testMap.remove(k, null, InternalOakHashTest::slowDeserialize).value));
        threadList.add(new Thread(() -> results[1] =
                (Integer) testMap.remove(k, null,
                OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize).value));

        runThreads(threadList);

        Assert.assertNotEquals(results[0], results[1]);

        // check that key was indeed deleted
        OakUnscopedBuffer vBuffer = testMap.get(k);
        Assert.assertNull(vBuffer);
    }

}
