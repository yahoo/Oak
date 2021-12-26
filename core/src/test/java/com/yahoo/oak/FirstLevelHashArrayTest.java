/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */


package com.yahoo.oak;


import com.yahoo.oak.common.integer.OakIntComparator;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;


public class FirstLevelHashArrayTest {

    FirstLevelHashArray<Integer, Integer> chunks;
    final int msbForFirstLevelHash = 3;
    final int lsbForSecondLevelHash = 9;
    @Before
    public void initArray() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
        SyncRecycleMemoryManager vMM = new SyncRecycleMemoryManager(allocator);
        SyncRecycleMemoryManager kMM = new SyncRecycleMemoryManager(allocator);

        OakComparator<Integer> comparator = new OakIntComparator();
        OakSerializer<Integer> keySerializer = new OakIntSerializer();
        OakSerializer<Integer> valueSerializer = new OakIntSerializer();
        int multipleReferenceNum = 2;
        chunks = new FirstLevelHashArray<>(new OakSharedConfig<>(
                allocator, kMM, vMM, keySerializer, valueSerializer, new OakIntComparator()
        ), msbForFirstLevelHash, lsbForSecondLevelHash, multipleReferenceNum);
    }

    private int setMsb(int numOfBits, int msbValue, int currentValue) {
        final int bitSizeofInt = 32;
        // do not touch the MSB
        int mask = ((1 << (numOfBits + 1)) - 1) << (bitSizeofInt - numOfBits - 1);
        int maskNot = ~mask;
        return (currentValue & maskNot) | (msbValue << (bitSizeofInt - numOfBits - 1));

    }

    @Test
    public void testGetChunk() {
        int expectedIdx = 1;
        int keyHash = setMsb(msbForFirstLevelHash, expectedIdx, 0);
        keyHash += 1234; // lsb are not important

        HashChunk<Integer, Integer> chunkByIdx = chunks.getChunk(expectedIdx);
        HashChunk<Integer, Integer> chunkByHash = chunks.findChunk(keyHash);
        Assert.assertEquals(chunkByHash, chunkByIdx);

        HashChunk<Integer, Integer> chunkEven = chunks.getChunk(2 * expectedIdx);
        HashChunk<Integer, Integer> chunkOdd = chunks.getChunk(2 * expectedIdx + 1);
        Assert.assertEquals(chunkOdd, chunkEven);
    }

    @Test
    public void testGetNextChunk() {
        int expectedIdx = 1;
        // verify getNextChunk by the current chunk
        HashChunk<Integer, Integer> nxtChunkByIdx = chunks.getChunk(2 * (expectedIdx + 1));
        HashChunk<Integer, Integer> currChunk = chunks.getChunk(2 * expectedIdx);
        BasicChunk<Integer, Integer> nxtChunkByChunk = chunks.getNextChunk(currChunk, 0, false);
        Assert.assertEquals(nxtChunkByIdx, nxtChunkByChunk);

        // verify getNextChunk by the hasCode
        nxtChunkByIdx = chunks.getChunk(2 * (expectedIdx + 1));
        currChunk = chunks.getChunk(2 * expectedIdx);
        int keyHashOdd = setMsb(msbForFirstLevelHash, 2 * expectedIdx, 0);
        int keyHashEven = setMsb(msbForFirstLevelHash, 2 * expectedIdx + 1, 0);
        BasicChunk<Integer, Integer> nxtChunkByHashOdd = chunks.getNextChunk(currChunk, keyHashOdd, true);
        BasicChunk<Integer, Integer> nxtChunkByHashEven = chunks.getNextChunk(currChunk, keyHashEven, true);
        Assert.assertEquals(nxtChunkByIdx, nxtChunkByHashOdd);
        Assert.assertEquals(nxtChunkByIdx, nxtChunkByHashEven);
    }
}
