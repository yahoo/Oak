/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleThreadIteratorTest {

    private OakMapOffHeapImpl<Integer, Integer> oak;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = 100;

    @Before
    public void init() {
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = builder.buildOffHeapOakMap();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testIterator() {
        Integer i;
        Integer value;
        for (i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }
        Iterator<Integer> valIter = oak.valuesIterator();
        Iterator<Map.Entry<Integer, Integer>> entryIter = oak.entriesIterator();
        i = 0;
        while (valIter.hasNext()) {
            assertEquals(i, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            assertEquals(i, e.getKey());
            assertEquals(i, e.getValue());
            i++;
        }
        for (i = 0; i < maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        for (i = 0; i < maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }
        valIter = oak.valuesIterator();
        entryIter = oak.entriesIterator();
        i = maxItemsPerChunk;
        while (valIter.hasNext()) {
            assertEquals(i, valIter.next());
            Map.Entry<Integer, Integer> e = entryIter.next();
            assertEquals(i, e.getValue());
            i++;
        }
        for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            oak.remove(i);
        }
        for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            value = oak.get(i);
            assertTrue(value == null);
        }
        for (i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (i = 1; i < (2 * maxItemsPerChunk - 1); i++) {
            oak.remove(i);
        }

        i = 0;
        value = oak.get(i);
        assertTrue(value != null);
        assertEquals(i, value);

        i = 2 * maxItemsPerChunk - 1;
        value = oak.get(i);
        assertTrue(value != null);
        assertEquals(i, value);

        valIter = oak.valuesIterator();
        assertTrue(valIter.hasNext());
        assertEquals((Integer) 0, valIter.next());
        assertTrue(valIter.hasNext());
        assertEquals(i, valIter.next());
    }

    @Test
    public void testGetRange() {
        OakMap sub = oak.subMap(0, true, 3 * maxItemsPerChunk, false);
        Iterator<Integer> iter = sub.valuesIterator();
        assertFalse(iter.hasNext());

        for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
            if (i % 3 == 0) {
                oak.remove(i);
            }
        }

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

        sub = oak.subMap(6 * maxItemsPerChunk, true, 9 * maxItemsPerChunk, false);
        iter = sub.valuesIterator();
        c = 6 * maxItemsPerChunk;
        while (iter.hasNext()) {
            if (c % 3 == 0) {
                c++;
            }
            assertEquals(c, iter.next());
            c++;
        }
        assertEquals(9 * maxItemsPerChunk, c.intValue());

        sub = oak.subMap(9 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false);
        iter = sub.valuesIterator();
        c = 9 * maxItemsPerChunk;
        while (iter.hasNext()) {
            if (c % 3 == 0) {
                c++;
            }
            assertEquals(c, iter.next());
            c++;
        }
        assertEquals(12 * maxItemsPerChunk, c.intValue());

        sub = oak.subMap(12 * maxItemsPerChunk, true, 13 * maxItemsPerChunk, false);
        iter = sub.valuesIterator();
        assertFalse(iter.hasNext());
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
        Iterator iter = oak.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals(i, iter.next());
        }
        assertEquals(0, i.intValue());
        OakMap map = oak.descendingMap();
        iter = map.descendingMap().valuesIterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(5, i.intValue());
        iter = map.entriesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            Map.Entry<Integer, Integer> e = (Map.Entry<Integer, Integer>) iter.next();
            assertEquals(i, e.getValue());
        }
        assertEquals(0, i.intValue());

        OakMap sub = oak.subMap(1, false, 4, true);
        iter = sub.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, iter.next());
        }
        assertEquals(2, i.intValue());
    }

    @Test
    public void testDescending() {
        Iterator iter = oak.descendingMap().valuesIterator();
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

        iter = oak.valuesIterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(5, i.intValue());

        iter = oak.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals(i, iter.next());
        }
        assertEquals(0, i.intValue());

        OakMap sub = oak.subMap(1, false, 4, true);
        iter = sub.valuesIterator();
        i = 2;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(5, i.intValue());

        iter = sub.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals(i, iter.next());
        }
        assertEquals(2, i.intValue());


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

        iter = oak.descendingMap().valuesIterator();
        i = 3 * maxItemsPerChunk;
        while (iter.hasNext()) {
            i--;
            assertEquals(i, iter.next());
        }
        assertEquals(0, i.intValue());

        sub = oak.subMap(2 * maxItemsPerChunk, true, 3 * maxItemsPerChunk, false);
        iter = sub.valuesIterator();
        i = 2 * maxItemsPerChunk;
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals(3 * maxItemsPerChunk, i.intValue());
        iter = sub.descendingMap().valuesIterator();
        i = 3 * maxItemsPerChunk;
        while (iter.hasNext()) {
            i--;
            assertEquals(i, iter.next());
        }
        assertEquals(2 * maxItemsPerChunk, i.intValue());

        sub = oak.subMap((int) Math.round(0.1 * maxItemsPerChunk), true, (int) Math.round(2.3 * maxItemsPerChunk), false);
        iter = sub.valuesIterator();
        i = (int) Math.round(0.1 * maxItemsPerChunk);
        while (iter.hasNext()) {
            assertEquals(i, iter.next());
            i++;
        }
        assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());
        iter = sub.descendingMap().valuesIterator();
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
        iter = sub.descendingMap().valuesIterator();
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
