/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.MyBuffer;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class OakMyBufferTest {

    OakMyBufferMap<MyBuffer, MyBuffer> oakMapBench;

    static final int SIZE = 1000;
    static final int RANGE = 2048;

    private static final ThreadLocal<Random> S_RANDOM = new ThreadLocal<Random>() {
        @Override
        protected synchronized Random initialValue() {
            return new Random();
        }
    };

    @Before
    public void init() {
        oakMapBench = new OakMyBufferMap<>();
    }

    @Test
    public void testPut() {
        for (long i = SIZE; i > 0; ) {
            int v = S_RANDOM.get().nextInt(RANGE);
            MyBuffer key = new MyBuffer(Parameters.confKeySize);
            key.buffer.putInt(0, v);
            MyBuffer val = new MyBuffer(Parameters.confValSize);
            val.buffer.putInt(0, v);
            if (oakMapBench.putIfAbsentOak(key, val)) {
                i--;
            }
        }
    }


    @Test
    public void testIncreasePut() {
        OakMyBufferHash<MyBuffer, MyBuffer> oakHashBench = new OakMyBufferHash<>(true);
        for (int v = 1; v < 100000; v++) {
            MyBuffer key = new MyBuffer(Parameters.confKeySize);
            key.buffer.putInt(0, v);
            MyBuffer val = new MyBuffer(Parameters.confValSize);
            val.buffer.putInt(0, v);
            oakMapBench.putOak(key, val);
            oakHashBench.putOak(key, val);
        }
        for (int v = 1; v < 100000; v++) {
            MyBuffer key = new MyBuffer(Parameters.confKeySize);
            key.buffer.putInt(0, v);
            MyBuffer val = new MyBuffer(Parameters.confValSize);
            val.buffer.putInt(0, v);
            assert oakMapBench.getOak(key);
            assert oakHashBench.getOak(key);
        }
    }

    @Test
    public void testPutMinimal() {
        MyBuffer key = new MyBuffer(Parameters.confKeySize);
        key.buffer.putInt(0, Integer.MIN_VALUE);
        MyBuffer val = new MyBuffer(Parameters.confValSize);
        val.buffer.putInt(0, Integer.MIN_VALUE);
        boolean success = oakMapBench.putIfAbsentOak(key, val);
        Assert.assertTrue(success);
    }
}
