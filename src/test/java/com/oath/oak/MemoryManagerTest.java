/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import javafx.util.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.logging.Logger;

public class MemoryManagerTest {

    Logger log = Logger.getLogger(MemoryManagerTest.class.getName());
    private SynchrobenchMemoryPoolImpl pool;
    private OakMemoryManager memoryManager;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = 100;

    public static class CheckOakCapacityValueSerializer implements Serializer<Integer> {

        @Override
        public void serialize(Integer value, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), value);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.getInt(serializedValue.position());
        }

        @Override
        public int calculateSize(Integer value) {
            return Integer.MAX_VALUE/20;
        }
    }

    @Before
    public void init() {
        pool = new SynchrobenchMemoryPoolImpl(100, maxItemsPerChunk, maxItemsPerChunk * maxBytesPerChunkItem);
        memoryManager = new OakMemoryManager(pool);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkCapacity() {

        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(4, pool.allocated());

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb1 = pair.getValue();
        assertEquals(4, bb1.remaining());
        assertEquals(8, pool.allocated());

        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb2 = pair.getValue();
        assertEquals(8, bb2.remaining());
        assertEquals(16, pool.allocated());

        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb3 = pair.getValue();
        assertEquals(8, bb3.remaining());
        assertEquals(24, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb4 = pair.getValue();
        assertEquals(8, bb4.remaining());
        assertEquals(32, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb5 = pair.getValue();
        assertEquals(8, bb5.remaining());
        assertEquals(40, pool.allocated());

        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb6 = pair.getValue();
        assertEquals(8, bb6.remaining());
        assertEquals(48, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb7 = pair.getValue();
        assertEquals(8, bb7.remaining());
        assertEquals(56, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb8 = pair.getValue();
        assertEquals(8, bb8.remaining());
        assertEquals(64, pool.allocated());

        pair = memoryManager.allocate(36);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb9 = pair.getValue();
        assertEquals(36, bb9.remaining());
        assertEquals(100, pool.allocated());

        thrown.expect(OakOutOfMemoryException.class);
        pair = memoryManager.allocate(1);
        assertEquals(0, (int) pair.getKey());
    }

    @Test
    public void checkOakCapacity() {

        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem)
                .setValueSerializer(new CheckOakCapacityValueSerializer());
        OakMap oak = (OakMap<Integer, Integer>) builder.build();
        MemoryPool pool = oak.getMemoryManager().pool;


        assertEquals(maxItemsPerChunk * maxBytesPerChunkItem, pool.allocated());
        Integer val = 1;
        Integer key = 0;
        assertEquals(maxItemsPerChunk * maxBytesPerChunkItem, pool.allocated());
        oak.put(key, val);
        key = 1;
        oak.put(key, val);
        key = 2;
        oak.put(key, val);
        key = 3;
        oak.put(key, val);


        key = 0;
        Integer value = (Integer) oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        key = 3;
        value = (Integer) oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);

        for(int i = 0; i < 10; i++){
            key = i;
            oak.put(key, val);
            oak.remove(key);
        }
    }

    @Test
    public void checkRelease() {

        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(4, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(8, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(12, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(16, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(20, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(24, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(28, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(32, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(36, pool.allocated());
        memoryManager.release(0, bb);

        assertEquals(9, memoryManager.releasedArray.get(1).size());

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(40, pool.allocated());
        memoryManager.release(0, bb);

        if(OakMemoryManager.RELEASES == 10)
            assertEquals(0, memoryManager.releasedArray.get(1).size());
    }

    @Test
    public void checkSingleThreadRelease() {

        assertEquals(0, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.attachThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.attachThread();
        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb = pair.getValue();
        bb.putInt(0,1);
        assertEquals(4, bb.remaining());
        assertEquals(4, pool.allocated());
        memoryManager.release(0, bb);
        memoryManager.detachThread();
        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.detachThread();
        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        memoryManager.attachThread();
        memoryManager.attachThread();
        memoryManager.attachThread();
        memoryManager.detachThread();
        memoryManager.detachThread();
        memoryManager.detachThread();

        memoryManager.attachThread();
        memoryManager.release(0, ByteBuffer.allocateDirect(4));
        memoryManager.detachThread();

        for(int i = 3 ; i < OakMemoryManager.RELEASES; i++){
            memoryManager.release(0, ByteBuffer.allocateDirect(4));
        }
        assertEquals(OakMemoryManager.RELEASES-1, memoryManager.releasedArray.get(1).size());
        memoryManager.attachThread();
        memoryManager.release(0, ByteBuffer.allocateDirect(4));
        memoryManager.detachThread();
        assertEquals(false, memoryManager.releasedArray.get(1).isEmpty());

    }

    @Test
    public void checkStartStopThread() {

        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        OakMap<Integer, Integer> oak = (OakMap<Integer, Integer>) builder.build();
        OakMemoryManager memoryManager = oak.getMemoryManager();

        assertEquals(0, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.attachThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.detachThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        Integer key = 0;
        oak.put(key, key);
        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        Integer value = (Integer) oak.get(key);
        assertEquals(3, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        assertTrue(value != null);
        assertEquals((Integer) 0, value);
        assertEquals(3, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        memoryManager.attachThread();
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.detachThread();
        assertEquals(4, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));


        memoryManager.attachThread();
        assertEquals(5, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.attachThread();
        assertEquals(6, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        oak.get(key);
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        memoryManager.detachThread();
        memoryManager.detachThread();
        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));


        oak.put(key, key);
        assertEquals(8, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        memoryManager.attachThread();
        assertEquals(9, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.attachThread();
        assertEquals(10, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        oak.put(key, key);
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        assertEquals(11, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        memoryManager.detachThread();
        memoryManager.detachThread();
        assertEquals(11, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));


        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        assertEquals(11 + 2 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        memoryManager.attachThread();
        assertEquals(12 + 2 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            oak.put(i, i);
        }
        assertEquals(12 + 4 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.detachThread();
        assertEquals(12 + 4 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
            value = (Integer) oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }

        try (CloseableIterator iter = oak.entriesIterator()) {
            assertEquals(13 + 6 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            Integer i = 0;
            while (iter.hasNext()) {
                Map.Entry<Integer, Integer> e = (Map.Entry<Integer, Integer>) iter.next();
                assertEquals(i, e.getValue());
                assertEquals(i, e.getKey());
                i++;
            }
            Integer twiceMaxItemsPerChunk = 2 * maxItemsPerChunk;
            assertEquals(twiceMaxItemsPerChunk, i);
        }

        try (CloseableIterator iter = oak.valuesIterator()) {
            assertEquals(14 + 8 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            int i = 0;
            while (iter.hasNext()) {
                assertEquals(i, iter.next());
                i++;
            }
            oak.get(0);
            assertEquals(2 * maxItemsPerChunk, i);
            assertEquals(15 + 10 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            try (CloseableIterator<Integer> ignored = oak.descendingMap().valuesIterator()) {
                assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            }
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        }
        assertEquals(16 + 10 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

    }

    @Test
    public void checkOneChunk() {

        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        OakMap oak = (OakMap<Integer, Integer>) builder.build();

        oak.put(128, 128);

        for (int i = 0; i < 270; i++) {
            oak.put(i, i);
        }

        for (Integer i = 0; i < 270; i++) {
            Integer value = (Integer) oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }

    }

    @Test
    public void releaseCalls() {

        int items = (maxItemsPerChunk * maxBytesPerChunkItem) / 4;
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        OakMap oak = (OakMap<Integer, Integer>) builder.build();
        OakMemoryManager memoryManager = oak.getMemoryManager();

        assertEquals(0, memoryManager.releasedArray.get(1).size());

        for (int i = 0; i < items; i++) {
            oak.put(i, i);
        }

        int releases = memoryManager.releasedArray.get(1).size();

        oak.remove(0);

        //assertEquals(releases + 1, memoryManager.releasedArray.get(1).size());

        oak.put(0, 0);
        assertEquals(releases + 1, memoryManager.releasedArray.get(1).size());

        for (int i = 0; i < 100; i++) {
            oak.remove(i);
            if(OakMemoryManager.RELEASES == 10) {
                if (i < 7) {
                    assertEquals(releases + 2 + i, memoryManager.releasedArray.get(1).size());
                } else { // TODO fix test
                    assertEquals((releases + 2 + i) % 10, memoryManager.releasedArray.get(1).size());
                }
            } else if (OakMemoryManager.RELEASES > 100){
                assertEquals(releases + 2 + i, memoryManager.releasedArray.get(1).size());
            }
        }

        int now = memoryManager.releasedArray.get(1).size();

        for (int i = 0; i < items; i++) {
            oak.put(i, i);
        }

        assertEquals(now, memoryManager.releasedArray.get(1).size());

        for (Integer i = 0; i < items; i++) {
            Integer value = (Integer) oak.get(i);
            assertTrue(value != null);
            assertEquals(i, value);
        }

        assertEquals(now, memoryManager.releasedArray.get(1).size());

    }

    @Test
    public void compute(){
        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        OakMap<Integer, Integer> oak = (OakMap<Integer, Integer>) builder.build();

        Consumer<OakWBuffer> computer = new Consumer<OakWBuffer>() {
            @Override
            public void accept(OakWBuffer oakWBuffer) {
                if (oakWBuffer.getInt(0) == 0) {
                    oakWBuffer.putInt(0, 1);
                }
            }
        };

        Integer key = 0;
        oak.put(key, key);
        Integer value = oak.get(key);
        assertTrue(value != null);
        assertEquals(key, value);
        oak.computeIfPresent(key, computer);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);
        oak.computeIfPresent(key, computer);
        value = oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);

    }


}
