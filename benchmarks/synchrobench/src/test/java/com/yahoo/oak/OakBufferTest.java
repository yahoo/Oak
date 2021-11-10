/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import com.yahoo.oak.synchrobench.data.buffer.KeyGen;
import com.yahoo.oak.synchrobench.data.buffer.KeyValueBuffer;
import com.yahoo.oak.synchrobench.data.buffer.ValueGen;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class OakBufferTest {

    OakBenchMap oakMapBench;
    OakBenchHash oakHashBench;

    static final int SIZE = 100_000;
    static final int RANGE = SIZE * 2;

    private static final ThreadLocal<Random> S_RANDOM = new ThreadLocal<Random>() {
        @Override
        protected synchronized Random initialValue() {
            return new Random();
        }
    };

    @Before
    public void init() {
        Parameters.confSmallFootprint = true;
        oakMapBench = new OakBenchMap(
            new KeyGen(Parameters.confKeySize),
            new ValueGen(Parameters.confValSize)
        );
        oakHashBench = new OakBenchHash(
            new KeyGen(Parameters.confKeySize),
            new ValueGen(Parameters.confValSize)
        );
        oakMapBench.init();
        oakHashBench.init();
    }

    @After
    public void tear() {
        oakMapBench.close();
        oakHashBench.close();
    }

    @Test
    public void testPut() {
        for (int i = 0; i < SIZE; i++) {
            int v = S_RANDOM.get().nextInt(RANGE);
            KeyValueBuffer key = new KeyValueBuffer(Parameters.confKeySize);
            key.buffer.putInt(0, v);
            KeyValueBuffer val = new KeyValueBuffer(Parameters.confValSize);
            val.buffer.putInt(0, v);
            oakMapBench.putIfAbsentOak(key, val);
            oakHashBench.putIfAbsentOak(key, val);
        }
    }


    @Test
    public void testIncreasePut() {
        for (int i = 0; i < SIZE; i++) {
            KeyValueBuffer key = new KeyValueBuffer(Parameters.confKeySize);
            key.buffer.putInt(0, i);
            KeyValueBuffer val = new KeyValueBuffer(Parameters.confValSize);
            val.buffer.putInt(0, i);
            oakMapBench.putOak(key, val);
            oakHashBench.putOak(key, val);
        }

        for (int i = 0; i < SIZE; i++) {
            KeyValueBuffer key = new KeyValueBuffer(Parameters.confKeySize);
            key.buffer.putInt(0, i);
            KeyValueBuffer val = new KeyValueBuffer(Parameters.confValSize);
            val.buffer.putInt(0, i);

            boolean success;
            success = oakMapBench.getOak(key, null);
            Assert.assertTrue(success);

            success = oakHashBench.getOak(key, null);
            Assert.assertTrue(success);
        }
    }

    @Test
    public void testPutMinimal() {
        KeyValueBuffer key = new KeyValueBuffer(Parameters.confKeySize);
        key.buffer.putInt(0, Integer.MIN_VALUE);
        KeyValueBuffer val = new KeyValueBuffer(Parameters.confValSize);
        val.buffer.putInt(0, Integer.MIN_VALUE);

        boolean success;

        success = oakMapBench.putIfAbsentOak(key, val);
        Assert.assertTrue(success);

        success = oakHashBench.putIfAbsentOak(key, val);
        Assert.assertTrue(success);
    }
}
