package com.oath.oak;

import com.oath.oak.common.OakCommonBuildersFactory;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class InternalOakMapTest {

    private InternalOakMap<Integer, Integer> testMap;

    private static final long operationDelay = 100;
    private static final long longTransformationDelay = 1000;

    @Before
    public void setUp() {
        NovaManager memoryManager = new NovaManager(new OakNativeMemoryAllocator(128));
        int chunkMaxItems = 100;

        testMap = new InternalOakMap<>(Integer.MIN_VALUE, OakCommonBuildersFactory.defaultIntSerializer,
            OakCommonBuildersFactory.defaultIntSerializer, OakCommonBuildersFactory.defaultIntComparator,
            memoryManager, chunkMaxItems, new ValueUtilsImpl());
    }


    private static Integer slowDeserialize(ByteBuffer bb) {
        try {
            Thread.sleep(longTransformationDelay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return OakCommonBuildersFactory.defaultIntSerializer.deserialize(bb);
    }

    private void runThreads(List<Thread> threadList) throws InterruptedException {
        for (Thread thread : threadList) {
            thread.start();
            Thread.sleep(operationDelay);
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
        threadList.add(new Thread(() -> results[0] = testMap.put(k, v1, OakCommonBuildersFactory.defaultIntSerializer::deserialize)));
        threadList.add(new Thread(() -> results[1] = testMap.put(k, v2, InternalOakMapTest::slowDeserialize)));
        threadList.add(new Thread(() -> results[2] = testMap.put(k, v3, OakCommonBuildersFactory.defaultIntSerializer::deserialize)));

        runThreads(threadList);

        assertNull(results[0]);
        assertNotEquals(results[1], results[2]);
    }

    @Test
    public void concurrentPutAndRemove() throws InterruptedException {
        Integer k = 1;
        Integer v1 = 1;
        Integer v2 = 2;

        final Integer[] results = new Integer[3];

        List<Thread> threadList = new ArrayList<>(results.length);
        threadList.add(new Thread(() -> results[0] = testMap.put(k, v1, OakCommonBuildersFactory.defaultIntSerializer::deserialize)));
        threadList.add(new Thread(() -> results[1] =
            (Integer) testMap.remove(k, null, InternalOakMapTest::slowDeserialize).value));
        threadList.add(new Thread(() -> results[2] = testMap.put(k, v2, OakCommonBuildersFactory.defaultIntSerializer::deserialize)));

        runThreads(threadList);

        assertNull(results[0]);
        assertNotEquals(results[1], results[2]);
    }

    @Test
    public void concurrentRemove() throws InterruptedException {
        Integer k = 1;
        Integer v1 = 1;

        final Integer[] results = new Integer[2];

        testMap.put(k, v1, OakCommonBuildersFactory.defaultIntSerializer::deserialize);

        List<Thread> threadList = new ArrayList<>(results.length);
        threadList.add(new Thread(() -> results[0] =
            (Integer) testMap.remove(k, null, InternalOakMapTest::slowDeserialize).value));
        threadList.add(new Thread(() -> results[1] =
            (Integer) testMap.remove(k, null, OakCommonBuildersFactory.defaultIntSerializer::deserialize).value));

        runThreads(threadList);

        assertNotEquals(results[0], results[1]);
    }
}