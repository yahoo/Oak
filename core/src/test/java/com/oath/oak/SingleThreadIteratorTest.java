/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleThreadIteratorTest {

    private OakMap<Integer, Integer> oak;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = 100;

    @Before
    public void init() {
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = (OakMap<Integer, Integer>) builder.build();
    }

    @After
    public void finish() throws Exception{
        oak.close();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testIterator() {
        Integer value;
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }
        oak.assertIfNotIdle();
        OakIterator<Integer> valIter = oak.valuesIterator();
        OakIterator<Map.Entry<Integer, Integer>> entryIter = oak.entriesIterator();
        Integer expectedVal = 0;
        while (valIter.hasNext()) {
            assertEquals(expectedVal, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            assertEquals(expectedVal, e.getKey());
            assertEquals(expectedVal, e.getValue());
            expectedVal++;
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        for (Integer i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertEquals(i, value);
        }

        oak.assertIfNotIdle();
        valIter = oak.valuesIterator();
        entryIter = oak.entriesIterator();

        expectedVal = maxItemsPerChunk;
        while (valIter.hasNext()) {
            assertEquals(expectedVal, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            assertEquals(expectedVal, e.getValue());
            expectedVal++;
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        for (Integer i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }

        oak.assertIfNotIdle();
        for (Integer i = 1; i < (2 * maxItemsPerChunk - 1); i++) {
            oak.remove(i);
        }

        expectedVal = 0;
        value = oak.get(expectedVal);
        assertEquals(expectedVal, value);

        expectedVal = 2 * maxItemsPerChunk - 1;
        value = oak.get(expectedVal);
        assertEquals(expectedVal, value);

        valIter = oak.valuesIterator();
        assertTrue(valIter.hasNext());
        assertEquals((Integer) 0, valIter.next());
        assertTrue(valIter.hasNext());
        assertEquals(expectedVal, valIter.next());
    }

    @Test
    public void testGetRange() {
        try (OakMap<Integer, Integer> sub = oak.subMap(0, true, 3 * maxItemsPerChunk, false)) {
            OakIterator<Integer> iter = sub.valuesIterator();
            assertFalse(iter.hasNext());

            for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
                oak.put(i, i);
            }
            for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
                if (i % 3 == 0) {
                    oak.remove(i);
                }
            }

            oak.assertIfNotIdle();
            iter = sub.valuesIterator();
            Integer c = 0;
            while (iter.hasNext()) {
                if (c % 3 == 0) {
                    c++;
                }
                assertEquals(c, iter.next());
                c++;
            }
            assertEquals(3 * maxItemsPerChunk, c.intValue());
            oak.assertIfNotIdle();
        }

        try (OakMap<Integer, Integer> sub = oak.subMap(6 * maxItemsPerChunk, true, 9 * maxItemsPerChunk, false)) {
            OakIterator<Integer> iter = sub.valuesIterator();
            Integer c = 6 * maxItemsPerChunk;
            while (iter.hasNext()) {
                if (c % 3 == 0) {
                    c++;
                }
                assertEquals(c, iter.next());
                c++;
            }
            assertEquals(9 * maxItemsPerChunk, c.intValue());
        }
        oak.assertIfNotIdle();
        try (OakMap<Integer, Integer> sub = oak.subMap(9 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false)){
            OakIterator<Integer> iter = sub.valuesIterator();
            Integer c = 9 * maxItemsPerChunk;
            while (iter.hasNext()) {
                if (c % 3 == 0) {
                    c++;
                }
                assertEquals(c, iter.next());
                c++;
            }
            assertEquals(12 * maxItemsPerChunk, c.intValue());
        }

        try (OakMap<Integer, Integer> sub = oak.subMap(12 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false);) {
            OakIterator<Integer> iter = sub.valuesIterator();
            assertFalse(iter.hasNext());
        }

    }

    @Test
    public void testEntryKeySet() {
        Integer i;
        Integer value;
        for (i = 0; i < 5; i++) {
            oak.put(i, i);
        }
        for (i = 0; i < 5; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }
        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap();) {

            OakIterator<Integer> iter = oakDesc.valuesIterator();
            i = 5;
            while (iter.hasNext()) {
                i--;
                assertEquals(i, iter.next());
            }
            assertEquals(0, i.intValue());
        }

        OakIterator<Integer> iter = oak.valuesIterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(5, i.intValue());



        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
            OakIterator<Map.Entry<Integer, Integer>> entryIter = oakDesc.entriesIterator();
            i = 5;
            while (entryIter.hasNext()) {
                i--;
                Map.Entry<Integer, Integer> e = (Map.Entry<Integer, Integer>) entryIter.next();
                assertEquals(i, e.getValue());
            }
            assertEquals(0, i.intValue());
        }



        try (OakMap<Integer, Integer> sub = oak.subMap(1, false, 4, true);
             OakMap<Integer, Integer> oakSubDesc = sub.descendingMap()) {
            OakIterator valIter = oakSubDesc.valuesIterator();
            i = 5;
            while (valIter.hasNext()) {
                i--;
                assertEquals((int) i, valIter.next());
            }
            assertEquals(2, i.intValue());
        }
    }

    @Test
    public void testDescending() {
        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {

            OakIterator iter = oakDesc.valuesIterator();
            assertFalse(iter.hasNext());

            Integer i;
            for (i = 0; i < 5; i++) {
                oak.put(i, i);
            }
            for (i = 0; i < 5; i++) {
                Integer value = oak.get(i);
                assertTrue(value != null);
                assertEquals(i, value);
            }
        }


        OakIterator<Integer> iter = oak.valuesIterator();

        Integer i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(5, i.intValue());



        try ( OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {

            OakIterator<Integer> valueiter = oakDesc.valuesIterator();
            i = 5;
            while (valueiter.hasNext()) {
                i--;
                assertEquals(i, valueiter.next());
            }
            assertEquals(0, i.intValue());
        }


        try (OakMap sub = oak.subMap(1, false, 4, true)) {
            iter = sub.valuesIterator();
            i = 2;
            while (iter.hasNext()) {
                assertEquals(i, iter.next());
                i++;
            }
            assertEquals(5, i.intValue());
        }


        try (OakMap oakSub = oak.subMap(1, false, 4, true);
             OakMap oakDesc = oakSub.descendingMap();) {

            iter = oakDesc.valuesIterator();

            i = 5;
            while (iter.hasNext()) {
                i--;
                assertEquals(i, iter.next());
            }
            assertEquals(2, i.intValue());
        }



        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            Integer value = oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }

        iter = oak.valuesIterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(3 * maxItemsPerChunk, i.intValue());



        try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
            iter = oakDesc.valuesIterator();
            i = 3 * maxItemsPerChunk;
            while (iter.hasNext()) {
                i--;
                assertEquals(i, iter.next());
            }
            assertEquals(0, i.intValue());
        }


        try (OakMap<Integer, Integer> sub = oak.subMap(2 * maxItemsPerChunk, true, 3 * maxItemsPerChunk, false);) {
            iter = sub.valuesIterator();

            i = 2 * maxItemsPerChunk;
            while (iter.hasNext()) {
                assertEquals(i, iter.next());
                i++;
            }
            assertEquals(3 * maxItemsPerChunk, i.intValue());


            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.valuesIterator();
                i = 3 * maxItemsPerChunk;
                while (iter.hasNext()) {
                    i--;
                    assertEquals(i, iter.next());
                }
                assertEquals(2 * maxItemsPerChunk, i.intValue());
            }
        }


        try (OakMap sub = oak.subMap((int) Math.round(0.1 * maxItemsPerChunk), true, (int) Math.round(2.3 * maxItemsPerChunk), false)){

            iter = sub.valuesIterator();
            i = (int) Math.round(0.1 * maxItemsPerChunk);
            while (iter.hasNext()) {
                assertEquals(i, iter.next());
                i++;
            }
            assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());

            try (OakMap<Integer, Integer> oakDesc = sub.descendingMap()) {
                iter = oakDesc.valuesIterator();

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
                    sub.remove(bb);
                }
            }


            iter = sub.valuesIterator();
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
                iter = oakDesc.valuesIterator();

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
