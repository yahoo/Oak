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

import java.util.Iterator;
import java.util.Map;

/**
 * Test to verify basic OakHash functionality
 */
public class SingleThreadIteratorTestHash {

    private OakHashMap<Integer, Integer> oak;
    private int maxItemsPerChunk = 2048;
    private int iteratorsRange = 10;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setOrderedChunkMaxItems(maxItemsPerChunk);
        oak = builder.buildHashMap();
    }

    @After
    public void finish() {
        oak.close();
    }

    private void populate(int numOfItems) {
        for (Integer i = 0; i < numOfItems; i++) {
            oak.zc().put(i, i);
        }
    }

    @Test
    public void basicSanityCheck() {
        Integer value;
        int numOfItems = 2 * maxItemsPerChunk;
        populate(numOfItems);

        for (Integer i = 0; i < numOfItems; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }
    }

    @Test
    public void testIterator() {
        Integer value;
        int numOfItems = 2 * maxItemsPerChunk;
        populate(numOfItems);

        Iterator<Integer> valIter = oak.values().iterator();
        Iterator<Map.Entry<Integer, Integer>> entryIter = oak.entrySet().iterator();
        boolean valuesPresent[] = new boolean[numOfItems];
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            Assert.assertFalse(valuesPresent[expectedVal]);
            valuesPresent[expectedVal] = true;

            Map.Entry<Integer, Integer> e = entryIter.next();
            Assert.assertEquals(expectedVal, e.getKey());
            Assert.assertEquals(expectedVal, e.getValue());
        }
        for (boolean flag:valuesPresent) {
            Assert.assertTrue(flag);
        }


        for (Integer i = 0; i < numOfItems / 2; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < numOfItems / 2; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }

        for (Integer i = numOfItems / 2; i < numOfItems; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }


        valIter = oak.values().iterator();
        entryIter = oak.entrySet().iterator();

        valuesPresent = new boolean[numOfItems];
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            Assert.assertFalse(valuesPresent[expectedVal]);
            valuesPresent[expectedVal] = true;

            Map.Entry<Integer, Integer> e = entryIter.next();
            Assert.assertEquals(expectedVal, e.getValue());
        }
        for (int index = 0; index < numOfItems; index++) {
            if ( index < numOfItems / 2) {
                Assert.assertFalse(valuesPresent[index]);
            } else {
                Assert.assertTrue(valuesPresent[index]);
            }
        }
        for (Integer i = numOfItems / 2; i < numOfItems; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = numOfItems / 2; i < numOfItems; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 0; i < numOfItems; i++) {
            oak.zc().put(i, i);
        }


        for (Integer i = 1; i < (numOfItems - 1); i++) {
            oak.zc().remove(i);
        }

        Integer expectedVal = 0;
        value = oak.get(expectedVal);
        Assert.assertEquals(expectedVal, value);

        expectedVal = numOfItems - 1;
        value = oak.get(expectedVal);
        Assert.assertEquals(expectedVal, value);

        valIter = oak.values().iterator();
        Assert.assertTrue(valIter.hasNext());
        Assert.assertEquals((Integer) 0, valIter.next());
        Assert.assertTrue(valIter.hasNext());
        Assert.assertEquals((Integer) (numOfItems - 1), valIter.next());
    }

    /**
     * Test for stream iterators. The test goes over the hash with three stream iterators in parralel
     * and verifies all is correct
     */
    @Test
    public void testStreamIterator() {
        int numOfItems = 2 * maxItemsPerChunk;
        populate(numOfItems);

        Iterator<OakUnscopedBuffer> keyStreamIterator = oak.zc().keyStreamSet().iterator();
        Iterator<OakUnscopedBuffer> valStreamIterator = oak.zc().valuesStream().iterator();
        Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entryStreamIterator
                = oak.zc().entryStreamSet().iterator();

        boolean valuesPresent[] = new boolean[numOfItems];

        while (keyStreamIterator.hasNext()) {
            Integer expectedKey = keyStreamIterator.next()
                    .transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Integer expectedVal = valStreamIterator.next()
                    .transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);

            Assert.assertFalse(valuesPresent[expectedKey]);
            Assert.assertEquals(expectedKey, expectedVal);

            Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> e = entryStreamIterator.next();
            Integer entryKey = e.getKey().transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Integer entryVal = e.getValue().transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);

            Assert.assertFalse(valuesPresent[entryKey]);
            Assert.assertEquals(entryKey, entryVal);

            valuesPresent[expectedVal] = true;

        }
        for (boolean flag:valuesPresent) {
            Assert.assertTrue(flag);
        }
    }

    @Test
    public void testIteratorRemove() {
        int numOfItems = 2 * maxItemsPerChunk;

        Integer valToRemove1 = 10;
        Integer valToRemove2 = 20;

        populate(numOfItems);

        Iterator<Integer> valIter = oak.values().iterator();
        Iterator<Map.Entry<Integer, Integer>> entryIter = oak.entrySet().iterator();
        boolean valuesPresent[] = new boolean[numOfItems];
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            Assert.assertFalse(valuesPresent[expectedVal]);
            valuesPresent[expectedVal] = true;

            Map.Entry<Integer, Integer> e = entryIter.next();

            if (expectedVal.equals(valToRemove1)) {
                valIter.remove();
            }
            if (expectedVal.equals(valToRemove2)) {
                entryIter.remove();
            }

            Assert.assertEquals(expectedVal, e.getKey());
            Assert.assertEquals(expectedVal, e.getValue());
        }
        for (boolean flag : valuesPresent) {
            Assert.assertTrue(flag);
        }


        // iterate over the remaining entries
        valIter = oak.values().iterator();
        entryIter = oak.entrySet().iterator();
        valuesPresent = new boolean[numOfItems];
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            Assert.assertFalse(valuesPresent[expectedVal]);
            valuesPresent[expectedVal] = true;

            Map.Entry<Integer, Integer> e = entryIter.next();

            Assert.assertEquals(expectedVal, e.getKey());
            Assert.assertEquals(expectedVal, e.getValue());
        }

        for (int idx = 0; idx < valuesPresent.length; idx++) {
            if (idx == valToRemove1 || idx == valToRemove2) {
                Assert.assertFalse(valuesPresent[idx]);
            } else {
                Assert.assertTrue(valuesPresent[idx]);
            }
        }
    }

    @Test
    public void testStreamIteratorsRemove() {
        int numOfItems = 2 * maxItemsPerChunk;

        Integer valToRemove1 = numOfItems / 4;
        Integer valToRemove2 = numOfItems / 2;


        populate(numOfItems);


        Iterator<OakUnscopedBuffer> keyStreamIterator = oak.zc().keyStreamSet().iterator();

        while (keyStreamIterator.hasNext()) {
            Integer currKey = keyStreamIterator.next()
                    .transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);


            if (currKey.equals(valToRemove1) || currKey.equals(valToRemove2)) {
                keyStreamIterator.remove();
            }
        }

        // iterate over the remaining entries

        Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entryStreamIterator
                = oak.zc().entryStreamSet().iterator();

        boolean valuesPresent[] = new boolean[numOfItems];

        while (entryStreamIterator.hasNext()) {

            Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> e = entryStreamIterator.next();
            Integer entryKey = e.getKey().transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);
            Integer entryVal = e.getValue().transform(OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER::deserialize);

            Assert.assertEquals(entryKey, entryVal);
            Assert.assertFalse(valuesPresent[entryKey]);
            valuesPresent[entryKey] = true;

        }

        for (int idx = 0; idx < valuesPresent.length; idx++) {
            if (idx == valToRemove1 || idx == valToRemove2) {
                Assert.assertFalse(valuesPresent[idx]);
            } else {
                Assert.assertTrue(valuesPresent[idx]);
            }
        }
    }
    @Test
    public void testIteratorRemoveCorners() {
        // @TODO remove the same entry the iterator points to
        int numOfItems = 200;
        int valueToRemove1 = 50;
        int valueToRemove2 = 100;

        populate(numOfItems);
        Iterator<Integer> valIter = oak.values().iterator();
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            if (expectedVal.equals(valueToRemove1)) {
                oak.remove(expectedVal);

                valIter.remove();
            }
        }

        valIter = oak.values().iterator();
        boolean valuesMet[] = new boolean[numOfItems];
        while (valIter.hasNext()) {
            valuesMet[valIter.next()] = true;
        }
        for (int idx = 0; idx < valuesMet.length; idx ++) {
            if (idx != valueToRemove1) {
                Assert.assertTrue(valuesMet[idx]);
            } else {
                Assert.assertFalse(valuesMet[idx]);
            }
        }

        // Two iterators running in parallel, the first removes the entry
        // the second is expected to return with next()

        Iterator<Integer> valIter1 = oak.values().iterator();
        Iterator<Integer> valIter2 = oak.values().iterator();
        valuesMet = new boolean[numOfItems];
        while (valIter1.hasNext()) {
            Integer expectedVal = valIter1.next();
            if (expectedVal.equals(valueToRemove2)) {
                valIter1.remove();
            }
            if (valIter1.hasNext()) {
                valuesMet[valIter2.next()] = true;
            }
        }
        for (int idx = 0; idx < valuesMet.length; idx ++) {
            if (idx != valueToRemove1 &&  idx != valueToRemove2) {
                Assert.assertTrue(valuesMet[idx]);
            } else {
                Assert.assertFalse(valuesMet[idx]);
            }
        }
    }

    /**
     * check how the iterator handles empty and sparse chunks
     */
    @Test
    public void testSparsePopulation() {
        int numOfItems = 10;
        populate(numOfItems);

        Iterator<Integer> valIter = oak.values().iterator();

        boolean valuesPresent[] = new boolean[numOfItems];
        while (valIter.hasNext()) {
            Integer expectedVal = valIter.next();
            Assert.assertFalse(valuesPresent[expectedVal]);
            valuesPresent[expectedVal] = true;

        }
        for (boolean flag:valuesPresent) {
            Assert.assertTrue(flag);
        }

        // test remove
        Iterator<Integer>  valIter1 = oak.values().iterator();

    }
}
