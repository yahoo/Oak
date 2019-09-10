/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleThreadIteratorTest {

    private OakMap<Integer, Integer> oak;
    private int maxItemsPerChunk = 2048;

    @Before
    public void init() {
        int maxBytesPerChunkItem = 100;
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
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
            assertEquals(i, value);
        }

        Iterator<Integer> valIter = oak.values().iterator();
        Iterator<Map.Entry<Integer, Integer>> entryIter = oak.entrySet().iterator();
        Integer expectedVal = 0;
        while (valIter.hasNext()) {
            assertEquals(expectedVal, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            assertEquals(expectedVal, e.getKey());
            assertEquals(expectedVal, e.getValue());
            expectedVal++;
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }


        valIter = oak.values().iterator();
        entryIter = oak.entrySet().iterator();

        expectedVal = maxItemsPerChunk;
        while (valIter.hasNext()) {
            assertEquals(expectedVal, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            assertEquals(expectedVal, e.getValue());
            expectedVal++;
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().remove(i);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertNull(value);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }


        for (Integer i = 1; i < (2 * maxItemsPerChunk - 1); i++) {
            oak.zc().remove(i);
        }

        expectedVal = 0;
        value = oak.get(expectedVal);
        assertEquals(expectedVal, value);

        expectedVal = 2 * maxItemsPerChunk - 1;
        value = oak.get(expectedVal);
        assertEquals(expectedVal, value);

        valIter = oak.values().iterator();
        assertTrue(valIter.hasNext());
        assertEquals((Integer) 0, valIter.next());
        assertTrue(valIter.hasNext());
        assertEquals(expectedVal, valIter.next());
    }

    @Test
    public void testGetRange() {
        try (OakMap<Integer, Integer> sub = oak.subMap(0, true, 3 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            assertFalse(iter.hasNext());

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
            assertEquals(3 * maxItemsPerChunk, c.intValue());

        }

        try (OakMap<Integer, Integer> sub = oak.subMap(6 * maxItemsPerChunk, true, 9 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            Integer c = 6 * maxItemsPerChunk;
            c = checkValues(iter, c);
            assertEquals(9 * maxItemsPerChunk, c.intValue());
        }

        try (OakMap<Integer, Integer> sub = oak.subMap(9 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            Integer c = 9 * maxItemsPerChunk;
            c = checkValues(iter, c);
            assertEquals(12 * maxItemsPerChunk, c.intValue());
        }

        try (OakMap<Integer, Integer> sub = oak.subMap(12 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false)) {
            Iterator<Integer> iter = sub.values().iterator();
            assertFalse(iter.hasNext());
        }

    }

    private Integer checkValues(Iterator<Integer> iter, Integer c) {
        while (iter.hasNext()) {
            if (c % 3 == 0) {
                c++;
            }
            assertEquals(c, iter.next());
            c++;
        }
        return c;
    }

    @Test
    public void testEntryKeySet() {
        Integer i;
        Integer value;
        for (i = 0; i < 5; i++) {
            oak.zc().put(i, i);
        }
        for (i = 0; i < 5; i++) {
            value = oak.get(i);
            assertNotNull(value);
            assertEquals(i, value);
        }
        checkDescendingIter();

        Iterator<Integer> iter = oak.values().iterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(5, i.intValue());


        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
            Iterator<Map.Entry<Integer, Integer>> entryIter = oakDesc.entrySet().iterator();
            i = 5;
            while (entryIter.hasNext()) {
                i--;
                Map.Entry<Integer, Integer> e = entryIter.next();
                assertEquals(i, e.getValue());
            }
            assertEquals(0, i.intValue());
        }


        try (OakMap<Integer, Integer> sub = oak.subMap(1, false, 4, true);
             OakMap<Integer, Integer> oakSubDesc = sub.descendingMap()) {
            Iterator valIter = oakSubDesc.values().iterator();
            i = 5;
            while (valIter.hasNext()) {
                i--;
                assertEquals(i, valIter.next());
            }
            assertEquals(2, i.intValue());
        }
    }

    private void checkDescendingIter() {
        Integer i;
        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {

            Iterator<Integer> iter = oakDesc.values().iterator();
            i = 5;
            while (iter.hasNext()) {
                i--;
                assertEquals(i, iter.next());
            }
            assertEquals(0, i.intValue());
        }
    }

    @Test
    public void testDescending() {
        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {

            Iterator iter = oakDesc.values().iterator();
            assertFalse(iter.hasNext());

            Integer i;
            for (i = 0; i < 5; i++) {
                oak.zc().put(i, i);
            }
            for (i = 0; i < 5; i++) {
                Integer value = oak.get(i);
                assertNotNull(value);
                assertEquals(i, value);
            }
        }


        Iterator<Integer> iter = oak.values().iterator();

        Integer i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(5, i.intValue());


        checkDescendingIter();


        try (OakMap<Integer, Integer> sub = oak.subMap(1, false, 4, true)) {
            iter = sub.values().iterator();
            i = 2;
            while (iter.hasNext()) {
                assertEquals(i, iter.next());
                i++;
            }
            assertEquals(5, i.intValue());
        }


        try (OakMap<Integer, Integer> oakSub = oak.subMap(1, false, 4, true);
             OakMap<Integer, Integer> oakDesc = oakSub.descendingMap()) {

            iter = oakDesc.values().iterator();

            i = 5;
            while (iter.hasNext()) {
                i--;
                assertEquals(i, iter.next());
            }
            assertEquals(2, i.intValue());
        }


        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            oak.zc().put(i, i);
        }
        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertNotNull(value);
            assertEquals(i, value);
        }

        iter = oak.values().iterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(3 * maxItemsPerChunk, i.intValue());


        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
            iter = oakDesc.values().iterator();
            i = 3 * maxItemsPerChunk;
            while (iter.hasNext()) {
                i--;
                assertEquals(i, iter.next());
            }
            assertEquals(0, i.intValue());
        }


        try (OakMap<Integer, Integer> sub = oak.subMap(2 * maxItemsPerChunk, true, 3 * maxItemsPerChunk, false)) {
            iter = sub.values().iterator();

            i = 2 * maxItemsPerChunk;
            while (iter.hasNext()) {
                assertEquals(i, iter.next());
                i++;
            }
            assertEquals(3 * maxItemsPerChunk, i.intValue());


            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.values().iterator();
                i = 3 * maxItemsPerChunk;
                while (iter.hasNext()) {
                    i--;
                    assertEquals(i, iter.next());
                }
                assertEquals(2 * maxItemsPerChunk, i.intValue());
            }
        }


        try (OakMap<Integer, Integer> sub = oak.subMap((int) Math.round(0.1 * maxItemsPerChunk), true, (int) Math.round(2.3 * maxItemsPerChunk), false)) {

            iter = sub.values().iterator();
            i = (int) Math.round(0.1 * maxItemsPerChunk);
            while (iter.hasNext()) {
                assertEquals(i, iter.next());
                i++;
            }
            assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());

            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.values().iterator();

                i = (int) Math.round(2.3 * maxItemsPerChunk);
                while (iter.hasNext()) {
                    i--;
                    assertEquals(i, iter.next());
                }
                assertEquals((int) Math.round(0.1 * maxItemsPerChunk), i.intValue());

                for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                    ByteBuffer bb = ByteBuffer.allocate(4);
                    bb.putInt(i);
                    bb.flip();
                    sub.zc().remove(bb);
                }
            }


            iter = sub.values().iterator();
            i = (int) Math.round(0.1 * maxItemsPerChunk);
            while (iter.hasNext()) {
                if (i == maxItemsPerChunk) {
                    i = 2 * maxItemsPerChunk;
                }
                assertEquals(i, iter.next());
                i++;
            }
            assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());


            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.values().iterator();

                i = (int) Math.round(2.3 * maxItemsPerChunk);
                while (iter.hasNext()) {
                    i--;
                    if (i == 2 * maxItemsPerChunk - 1) {
                        i = maxItemsPerChunk - 1;
                    }
                    assertEquals(i, iter.next());
                }
                assertEquals((int) Math.round(0.1 * maxItemsPerChunk), i.intValue());
            }
        }


    }

}
