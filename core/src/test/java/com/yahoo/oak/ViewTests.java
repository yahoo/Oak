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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class ViewTests {

    private OakMap<String, String> oak;
    private static final int ELEMENTS = 1000;

    @Before
    public void init() {
        OakMapBuilder<String, String> builder = OakCommonBuildersFactory.getDefaultStringBuilder()
            .setChunkMaxItems(100);

        oak = builder.build();

        for (int i = 0; i < ELEMENTS; i++) {
            String key = String.valueOf(i);
            String val = String.valueOf(i);
            oak.zc().put(key, val);
        }
    }

    @After
    public void tearDown() {
        oak.close();
    }

    @Test
    public void testOakUnscopedBuffer() {
        String testVal = String.valueOf(123);
        ByteBuffer testValBB = ByteBuffer.allocate(Integer.BYTES + testVal.length() * Character.BYTES);
        testValBB.putInt(0, testVal.length());
        for (int i = 0; i < testVal.length(); ++i) {
            testValBB.putChar(Integer.BYTES + i * Character.BYTES, testVal.charAt(i));
        }

        OakUnscopedBuffer valBuffer = oak.zc().get(testVal);
        String transformed = valBuffer.transform(OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
        Assert.assertEquals(testVal, transformed);

        Assert.assertEquals(testValBB.capacity(), valBuffer.capacity());

        Assert.assertEquals(testValBB.getInt(0), valBuffer.getInt(0));
        for (int i = 0; i < testVal.length(); i++) {
            int pos = Integer.BYTES + i * Character.BYTES;
            Assert.assertEquals(testValBB.getChar(pos), valBuffer.getChar(pos));
        }

        for (int i = 0; i < testVal.length(); ++i) {
            int pos = Integer.BYTES + i * Character.BYTES;
            Assert.assertEquals(testVal.charAt(i), valBuffer.getChar(pos));
        }
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testOakRBufferConcurrency() {
        String testVal = "987";
        OakUnscopedBuffer valBuffer = oak.zc().get(testVal);
        oak.zc().remove(testVal);
        valBuffer.get(0);
    }

    @Test
    public void testBufferViewAPIs() {
        String[] values = new String[ELEMENTS];
        for (int i = 0; i < ELEMENTS; i++) {
            values[i] = String.valueOf(i);
        }
        Arrays.sort(values);

        Iterator<OakUnscopedBuffer> keyIterator = oak.zc().keySet().iterator();
        for (int i = 0; i < ELEMENTS; i++) {
            OakUnscopedBuffer keyBB = keyIterator.next();
            String key = keyBB.transform(OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
            Assert.assertEquals(values[i], key);
        }

        Iterator<OakUnscopedBuffer> valueIterator = oak.zc().values().iterator();
        for (int i = 0; i < ELEMENTS; i++) {
            OakUnscopedBuffer valueBB = valueIterator.next();
            String value = valueBB.transform(OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
            Assert.assertEquals(values[i], value);
        }

        Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entryIterator = oak.zc().entrySet().iterator();
        for (int i = 0; i < ELEMENTS; i++) {
            Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> entryBB = entryIterator.next();
            String value = entryBB.getValue().transform(
                    OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
            Assert.assertEquals(values[i], value);
        }
    }

    @Test
    public void testStreamAPIs() {
        String[] values = new String[ELEMENTS];
        for (int i = 0; i < ELEMENTS; i++) {
            values[i] = String.valueOf(i);
        }
        Arrays.sort(values);

        Iterator<OakUnscopedBuffer> keyStreamIterator = oak.zc().keyStreamSet().iterator();
        for (int i = 0; i < ELEMENTS; i++) {
            OakUnscopedBuffer keyBB = keyStreamIterator.next();
            String key = keyBB.transform(OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
            Assert.assertEquals(values[i], key);
        }

        Iterator<OakUnscopedBuffer> valueStreamIterator = oak.zc().valuesStream().iterator();
        for (int i = 0; i < ELEMENTS; i++) {
            OakUnscopedBuffer valueBB = valueStreamIterator.next();
            String value = valueBB.transform(OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
            Assert.assertEquals(values[i], value);
        }

        Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entryStreamIterator
                = oak.zc().entryStreamSet().iterator();
        for (int i = 0; i < ELEMENTS; i++) {
            Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> entryBB = entryStreamIterator.next();
            String value = entryBB.getValue().transform(
                    OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
            Assert.assertEquals(values[i], value);
        }
    }

    @Test
    public void testStreamRangeAPIs() {
        String[] values = new String[ELEMENTS];
        for (int i = 0; i < ELEMENTS; i++) {
            values[i] = String.valueOf(i);
        }
        Arrays.sort(values);

        final int start = 2;
        final int end = ELEMENTS - 1;

        try (OakMap<String, String> sub = oak.subMap(values[start], true, values[end], false)) {

            Iterator<OakUnscopedBuffer> keyStreamIterator = sub.zc().keyStreamSet().iterator();
            for (int i = start; i < end; i++) {
                OakUnscopedBuffer keyBB = keyStreamIterator.next();
                String key = keyBB.transform(OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
                Assert.assertEquals(values[i], key);
            }

            Iterator<OakUnscopedBuffer> valueStreamIterator = sub.zc().valuesStream().iterator();
            for (int i = start; i < end; i++) {
                OakUnscopedBuffer valueBB = valueStreamIterator.next();
                String value = valueBB.transform(OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
                Assert.assertEquals(values[i], value);
            }

            Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entryStreamIterator
                    = sub.zc().entryStreamSet().iterator();
            for (int i = start; i < end; i++) {
                Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> entryBB = entryStreamIterator.next();
                String value = entryBB.getValue().transform(
                        OakCommonBuildersFactory.DEFAULT_STRING_SERIALIZER::deserialize);
                Assert.assertEquals(values[i], value);
            }
        }
    }

    @Test
    public void testTransformViewAPIs() {
        Function<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>, Integer> transformer = (entry) -> {
            Assert.assertNotNull(entry.getKey());
            Assert.assertNotNull(entry.getValue());
            int size = entry.getValue().getInt(0);
            StringBuilder object = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = entry.getValue().getChar(Integer.BYTES + i * Character.BYTES);
                object.append(c);
            }
            return Integer.valueOf(object.toString());
        };


        String[] values = new String[ELEMENTS];
        for (int i = 0; i < ELEMENTS; i++) {
            values[i] = String.valueOf(i);
        }
        Arrays.sort(values);

        Iterator<Integer> entryIterator = oak.zc().entrySet().stream().map(transformer).iterator();
        for (int i = 0; i < ELEMENTS; i++) {
            Integer entryT = entryIterator.next();
            Assert.assertEquals(Integer.valueOf(values[i]), entryT);
        }
    }
}
