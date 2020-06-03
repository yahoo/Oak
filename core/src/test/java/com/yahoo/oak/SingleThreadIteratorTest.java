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
import java.util.Iterator;
import java.util.Map;

public class SingleThreadIteratorTest {

    private OakMap<Integer, Integer> oak;
    private int maxItemsPerChunk = 2048;
    private int iteratorsRange = 10;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setChunkMaxItems(maxItemsPerChunk);
        oak = builder.build();
    }

    @After
    public void finish() {
        oak.close();
    }

    @Test
    public void testIterator() {
        Integer value;
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }

        Iterator<Integer> valIter = oak.values().iterator();
        Iterator<Map.Entry<Integer, Integer>> entryIter = oak.entrySet().iterator();
        Integer expectedVal = 0;
        while (valIter.hasNext()) {
            Assert.assertEquals(expectedVal, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            Assert.assertEquals(expectedVal, e.getKey());
            Assert.assertEquals(expectedVal, e.getValue());
            expectedVal++;
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertEquals(i, value);
        }


        valIter = oak.values().iterator();
        entryIter = oak.entrySet().iterator();

        expectedVal = maxItemsPerChunk;
        while (valIter.hasNext()) {
            Assert.assertEquals(expectedVal, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            Assert.assertEquals(expectedVal, e.getValue());
            expectedVal++;
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            Assert.assertNull(value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }


        for (Integer i = 1; i < (2 * maxItemsPerChunk - 1); i++) {
            oak.zc().remove(i);
        }

        expectedVal = 0;
        value = oak.get(expectedVal);
        Assert.assertEquals(expectedVal, value);

        expectedVal = 2 * maxItemsPerChunk - 1;
        value = oak.get(expectedVal);
        Assert.assertEquals(expectedVal, value);

        valIter = oak.values().iterator();
        Assert.assertTrue(valIter.hasNext());
        Assert.assertEquals((Integer) 0, valIter.next());
        Assert.assertTrue(valIter.hasNext());
        Assert.assertEquals(expectedVal, valIter.next());
    }

    @Test
    public void testGetRange() {
        try (OakMap<Integer, Integer> sub = oak.subMap(0, true, 3 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            Assert.assertFalse(iter.hasNext());

            for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
                oak.zc().put(i, i);
            }
            for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
                if (i % 3 == 0) {
                    oak.zc().remove(i);
                }
            }


            iter = sub.values().iterator();
            Integer c = 0;
            c = checkValues(iter, c);
            Assert.assertEquals(3 * maxItemsPerChunk, c.intValue());

        }

        try (OakMap<Integer, Integer> sub = oak.subMap(6 * maxItemsPerChunk, true, 9 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            Integer c = 6 * maxItemsPerChunk;
            c = checkValues(iter, c);
            Assert.assertEquals(9 * maxItemsPerChunk, c.intValue());
        }

        try (OakMap<Integer, Integer> sub = oak.subMap(9 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            Integer c = 9 * maxItemsPerChunk;
            c = checkValues(iter, c);
            Assert.assertEquals(12 * maxItemsPerChunk, c.intValue());
        }

        try (OakMap<Integer, Integer> sub = oak.subMap(12 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            Assert.assertFalse(iter.hasNext());
        }

    }

    private int checkValues(Iterator<Integer> iter, int inputInt) {
        int c = inputInt;
        while (iter.hasNext()) {
            if (c % 3 == 0) {
                c++;
            }
            Assert.assertEquals(c, (int) iter.next());
            c++;
        }
        return c;
    }

    @Test
    public void testEntryKeySet() {
        Integer i;
        Integer value;
        for (i = 0; i < iteratorsRange; i++) {
            oak.zc().put(i, i);
        }
        for (i = 0; i < iteratorsRange; i++) {
            value = oak.get(i);
            Assert.assertNotNull(value);
            Assert.assertEquals(i, value);
        }
        checkDescendingIter();

        Iterator<Integer> iter = oak.values().iterator();
        i = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(i, iter.next());
            i++;
        }
        Assert.assertEquals(iteratorsRange, i.intValue());


        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
            Iterator<Map.Entry<Integer, Integer>> entryIter = oakDesc.entrySet().iterator();
            i = iteratorsRange;
            while (entryIter.hasNext()) {
                i--;
                Map.Entry<Integer, Integer> e = entryIter.next();
                Assert.assertEquals(i, e.getValue());
            }
            Assert.assertEquals(0, i.intValue());
        }


        try (OakMap<Integer, Integer> sub = oak.subMap(1, false, iteratorsRange - 1, true);
             OakMap<Integer, Integer> oakSubDesc = sub.descendingMap()) {
            Iterator valIter = oakSubDesc.values().iterator();
            i = iteratorsRange;
            while (valIter.hasNext()) {
                i--;
                Assert.assertEquals(i, valIter.next());
            }
            Assert.assertEquals(2, i.intValue());
        }
    }

    private void checkDescendingIter() {
        Integer i;
        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {

            Iterator<Integer> iter = oakDesc.values().iterator();
            i = iteratorsRange;
            while (iter.hasNext()) {
                i--;
                Assert.assertEquals(i, iter.next());
            }
            Assert.assertEquals(0, i.intValue());
        }
    }

    @Test(timeout = 100000)
    public void testRandomDescending() {
        // it is important to test different distribution of inserted keys, not only increasing
        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {

            Iterator iter = oakDesc.values().iterator();
            Assert.assertFalse(iter.hasNext());

            Integer i;
            for (i = 0; i < iteratorsRange; i++) { // first insert even keys
                if (i % 2 == 0) {
                    oak.zc().put(i, i);
                }
            }
            for (i = 0; i < iteratorsRange; i++) { // then insert odd keys
                if (i % 2 == 1) {
                    oak.zc().put(i, i);
                }
            }
            for (i = 0; i < iteratorsRange; i++) {
                Integer value = oak.get(i);
                Assert.assertNotNull(value);
                Assert.assertEquals(i, value);
            }
        }

        Iterator<Integer> iter = oak.values().iterator();

        Integer i = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(i, iter.next());
            i++;
        }
        Assert.assertEquals(iteratorsRange, i.intValue());

        checkDescendingIter();

        try (OakMap<Integer, Integer> oakSub = oak.subMap(1, false, iteratorsRange - 1, true);
             OakMap<Integer, Integer> oakDesc = oakSub.descendingMap()) {

            iter = oakDesc.values().iterator();

            i = iteratorsRange;
            while (iter.hasNext()) {
                i--;
                Assert.assertEquals(i, iter.next());
            }
            Assert.assertEquals(2, i.intValue());
        }

        // test split chunks
        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            if (i % 2 == 0) {
                oak.zc().put(i, i);
            }
        }
        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            if (i % 2 == 1) {
                oak.zc().put(i, i);
            }
        }
        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            if (value == null) {
                assert false;
            }
            Assert.assertNotNull(value);
            Assert.assertEquals(i, value);
        }

        iter = oak.values().iterator();
        i = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(i, iter.next());
            i++;
        }
        Assert.assertEquals(3 * maxItemsPerChunk, i.intValue());


        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
            iter = oakDesc.values().iterator();
            i = 3 * maxItemsPerChunk;
            while (iter.hasNext()) {
                i--;
                Assert.assertEquals(i, iter.next());
            }
            Assert.assertEquals(0, i.intValue());
        }
    }

    @Test
    public void testDescending() {
        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {

            Iterator iter = oakDesc.values().iterator();
            Assert.assertFalse(iter.hasNext());

            Integer i;
            for (i = 0; i < iteratorsRange; i++) {
                oak.zc().put(i, i);
            }
            for (i = 0; i < iteratorsRange; i++) {
                Integer value = oak.get(i);
                Assert.assertNotNull(value);
                Assert.assertEquals(i, value);
            }
        }


        Iterator<Integer> iter = oak.values().iterator();

        Integer i = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(i, iter.next());
            i++;
        }
        Assert.assertEquals(iteratorsRange, i.intValue());


        checkDescendingIter();


        try (OakMap<Integer, Integer> sub = oak.subMap(1, false, iteratorsRange - 1, true)) {
            iter = sub.values().iterator();
            i = 2;
            while (iter.hasNext()) {
                Assert.assertEquals(i, iter.next());
                i++;
            }
            Assert.assertEquals(iteratorsRange, i.intValue());
        }


        try (OakMap<Integer, Integer> oakSub = oak.subMap(1, false, iteratorsRange - 1, true);
             OakMap<Integer, Integer> oakDesc = oakSub.descendingMap()) {

            iter = oakDesc.values().iterator();

            i = iteratorsRange;
            while (iter.hasNext()) {
                i--;
                Assert.assertEquals(i, iter.next());
            }
            Assert.assertEquals(2, i.intValue());
        }


        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            Assert.assertNotNull(value);
            Assert.assertEquals(i, value);
        }

        iter = oak.values().iterator();
        i = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(i, iter.next());
            i++;
        }
        Assert.assertEquals(3 * maxItemsPerChunk, i.intValue());


        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
            iter = oakDesc.values().iterator();
            i = 3 * maxItemsPerChunk;
            while (iter.hasNext()) {
                i--;
                Assert.assertEquals(i, iter.next());
            }
            Assert.assertEquals(0, i.intValue());
        }


        try (OakMap<Integer, Integer> sub = oak.subMap(2 * maxItemsPerChunk, true, 3 * maxItemsPerChunk, false)) {
            iter = sub.values().iterator();

            i = 2 * maxItemsPerChunk;
            while (iter.hasNext()) {
                Assert.assertEquals(i, iter.next());
                i++;
            }
            Assert.assertEquals(3 * maxItemsPerChunk, i.intValue());


            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.values().iterator();
                i = 3 * maxItemsPerChunk;
                while (iter.hasNext()) {
                    i--;
                    Assert.assertEquals(i, iter.next());
                }
                Assert.assertEquals(2 * maxItemsPerChunk, i.intValue());
            }
        }


        try (OakMap<Integer, Integer> sub = oak.subMap((int) Math.round(0.1 * maxItemsPerChunk), true,
                (int) Math.round(2.3 * maxItemsPerChunk), false)) {

            iter = sub.values().iterator();
            i = (int) Math.round(0.1 * maxItemsPerChunk);
            while (iter.hasNext()) {
                Assert.assertEquals(i, iter.next());
                i++;
            }
            Assert.assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());

            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.values().iterator();

                i = (int) Math.round(2.3 * maxItemsPerChunk);
                while (iter.hasNext()) {
                    i--;
                    Assert.assertEquals(i, iter.next());
                }
                Assert.assertEquals((int) Math.round(0.1 * maxItemsPerChunk), i.intValue());

                for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                    ByteBuffer bb = ByteBuffer.allocate(4);
                    bb.putInt(i);
                    bb.flip();
                    sub.zc().remove(i);
                }
            }


            iter = sub.values().iterator();
            i = (int) Math.round(0.1 * maxItemsPerChunk);
            while (iter.hasNext()) {
                if (i == maxItemsPerChunk) {
                    i = 2 * maxItemsPerChunk;
                }
                Assert.assertEquals(i, iter.next());
                i++;
            }
            Assert.assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());


            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.values().iterator();

                i = (int) Math.round(2.3 * maxItemsPerChunk);
                while (iter.hasNext()) {
                    i--;
                    if (i == 2 * maxItemsPerChunk - 1) {
                        i = maxItemsPerChunk - 1;
                    }
                    Assert.assertEquals(i, iter.next());
                }
                Assert.assertEquals((int) Math.round(0.1 * maxItemsPerChunk), i.intValue());
            }
        }


    }

}
