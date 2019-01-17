/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class OakMemoryManagerTest {
    private OakMemoryAllocator memoryAllocator;
    private ThreadIndexCalculator indexCalculator;
    private MemoryManager memoryManager;
    private long allocatedBytes;

    @Before
    public void setUp() {
        allocatedBytes = 0;
        indexCalculator = mock(ThreadIndexCalculator.class);
        when(indexCalculator.getIndex()).thenReturn(7);
        memoryAllocator = mock(OakMemoryAllocator.class);
        when(memoryAllocator.allocate(anyInt())).thenAnswer((Answer) invocation -> {
            int size = (int) invocation.getArguments()[0];
            allocatedBytes += size;
            return ByteBuffer.allocate(size);

        });
        doAnswer(invocation -> {
            ByteBuffer bb = (ByteBuffer) invocation.getArguments()[0];
            allocatedBytes -= bb.capacity();
            return  allocatedBytes;
        }).when(memoryAllocator).free(any());
        when(memoryAllocator.allocated()).thenAnswer((Answer) invocationOnMock -> allocatedBytes);
        memoryManager = new MemoryManager(memoryAllocator, indexCalculator);
    }

    @Test
    public void allocate() {
        ByteBuffer bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(4, memoryManager.allocated());

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(8, memoryManager.allocated());
    }

    @Test
    public void release() {
        memoryManager.setGCtrigger(2);

        ByteBuffer bb1 = memoryManager.allocate(4);
        ByteBuffer bb2 = memoryManager.allocate(4);

        memoryManager.release(bb1);
        verify(memoryAllocator, times(0)).free(any());
        assertEquals(8, memoryManager.allocated());

        memoryManager.release(bb2);
        verify(memoryAllocator, times(2)).free(any());
        assertEquals(0, memoryManager.allocated());
    }


    @Test
    public void close() {
        memoryManager.setGCtrigger(2);

        ByteBuffer bb1 = memoryManager.allocate(4);
        memoryManager.release(bb1);
        verify(memoryAllocator, times(0)).free(any());
        assertEquals(4, memoryManager.allocated());

        memoryManager.close();
        verify(memoryAllocator, times(1)).free(any());
        assertEquals(0, memoryManager.allocated());
    }

//
//    @Test
//    public void checkSingleThreadRelease() {
//
//        assertEquals(0, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.startOperation();
//        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.startOperation();
//        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(4);
//        assertEquals(0, (int) pair.getKey());
//        ByteBuffer bb = pair.getValue();
//        bb.putInt(0,1);
//        assertEquals(4, bb.remaining());
//        assertEquals(4, pool.allocated());
//        memoryManager.release(0, bb);
//        memoryManager.stopOperation();
//        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.stopOperation();
//        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//        memoryManager.startOperation();
//        memoryManager.startOperation();
//        memoryManager.startOperation();
//        memoryManager.stopOperation();
//        memoryManager.stopOperation();
//        memoryManager.stopOperation();
//
//        memoryManager.startOperation();
//        memoryManager.release(0, ByteBuffer.allocateDirect(4));
//        memoryManager.stopOperation();
//
//        for(int i = 3 ; i < OakMemoryManager.RELEASES_DEFAULT; i++){
//            memoryManager.release(0, ByteBuffer.allocateDirect(4));
//        }
//        assertEquals(OakMemoryManager.RELEASES_DEFAULT-1, memoryManager.releasedArray.get(1).size());
//        memoryManager.startOperation();
//        memoryManager.release(0, ByteBuffer.allocateDirect(4));
//        memoryManager.stopOperation();
//        assertEquals(false, memoryManager.releasedArray.get(1).isEmpty());
//
//    }
//
//    @Test
//    public void checkStartStopThread() {
//
//        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
//                .setChunkMaxItems(maxItemsPerChunk)
//                .setChunkBytesPerItem(maxBytesPerChunkItem);
//        OakMap<Integer, Integer> oak = (OakMap<Integer, Integer>) builder.build();
//        OakMemoryManager memoryManager = oak.getMemoryManager();
//
//        assertEquals(0, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.startOperation();
//        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.stopOperation();
//        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//        Integer key = 0;
//        oak.put(key, key);
//        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//        Integer value = (Integer) oak.get(key);
//        assertEquals(3, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        assertTrue(value != null);
//        assertEquals((Integer) 0, value);
//        assertEquals(3, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//        memoryManager.startOperation();
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.stopOperation();
//        assertEquals(4, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//
//        memoryManager.startOperation();
//        assertEquals(5, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.startOperation();
//        assertEquals(6, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        oak.get(key);
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        memoryManager.stopOperation();
//        memoryManager.stopOperation();
//        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//
//        oak.put(key, key);
//        assertEquals(8, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        memoryManager.startOperation();
//        assertEquals(9, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.startOperation();
//        assertEquals(10, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        oak.put(key, key);
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        assertEquals(11, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        memoryManager.stopOperation();
//        memoryManager.stopOperation();
//        assertEquals(11, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//
//        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
//            oak.put(i, i);
//        }
//        assertEquals(11 + 2 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//        memoryManager.startOperation();
//        assertEquals(12 + 2 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
//            oak.put(i, i);
//        }
//        assertEquals(12 + 4 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        memoryManager.stopOperation();
//        assertEquals(12 + 4 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//        for (Integer i = 0; i < 2 * maxItemsPerChunk; i++) {
//            value = (Integer) oak.get(i);
//            assertTrue(value != null);
//            assertEquals(i, value);
//        }
//
//        try (OakCloseableIterator iter = oak.entriesIterator()) {
//            assertEquals(13 + 6 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//            Integer i = 0;
//            while (iter.hasNext()) {
//                Map.Entry<Integer, Integer> e = (Map.Entry<Integer, Integer>) iter.next();
//                assertEquals(i, e.getValue());
//                assertEquals(i, e.getKey());
//                i++;
//            }
//            Integer twiceMaxItemsPerChunk = 2 * maxItemsPerChunk;
//            assertEquals(twiceMaxItemsPerChunk, i);
//        }
//
//        try (OakCloseableIterator iter = oak.valuesIterator()) {
//            assertEquals(14 + 8 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//            int i = 0;
//            while (iter.hasNext()) {
//                assertEquals(i, iter.next());
//                i++;
//            }
//            oak.get(0);
//            assertEquals(2 * maxItemsPerChunk, i);
//            assertEquals(15 + 10 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//            try (OakCloseableIterator<Integer> ignored = oak.descendingMap().valuesIterator()) {
//                assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//            }
//            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//        }
//        assertEquals(16 + 10 * maxItemsPerChunk, memoryManager.getValue(memoryManager.timeStamps[1].get()));
//        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
//
//    }
//
//    @Test
//    public void checkOneChunk() {
//
//        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
//                .setChunkMaxItems(maxItemsPerChunk)
//                .setChunkBytesPerItem(maxBytesPerChunkItem);
//        OakMap oak = (OakMap<Integer, Integer>) builder.build();
//
//        oak.put(128, 128);
//
//        for (int i = 0; i < 270; i++) {
//            oak.put(i, i);
//        }
//
//        for (Integer i = 0; i < 270; i++) {
//            Integer value = (Integer) oak.get(i);
//            assertTrue(value != null);
//            assertEquals(i, value);
//        }
//        oak.close();
//    }
//
//    @Test
//    public void releaseCalls() {
//
//        int items = (maxItemsPerChunk * maxBytesPerChunkItem) / 4;
//        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
//                .setChunkMaxItems(maxItemsPerChunk)
//                .setChunkBytesPerItem(maxBytesPerChunkItem);
//        OakMap oak = (OakMap<Integer, Integer>) builder.build();
//        OakMemoryManager memoryManager = oak.getMemoryManager();
//
//        assertEquals(0, memoryManager.releasedArray.get(1).size());
//
//        for (int i = 0; i < items; i++) {
//            oak.put(i, i);
//        }
//
//        int releases = memoryManager.releasedArray.get(1).size();
//
//        oak.remove(0);
//
//        //assertEquals(releases + 1, memoryManager.releasedArray.get(1).size());
//
//        oak.put(0, 0);
//        assertEquals(releases + 1, memoryManager.releasedArray.get(1).size());
//
//        for (int i = 0; i < 100; i++) {
//            oak.remove(i);
//            if(OakMemoryManager.RELEASES_DEFAULT == 10) {
//                if (i < 7) {
//                    assertEquals(releases + 2 + i, memoryManager.releasedArray.get(1).size());
//                } else { // TODO fix test
//                    assertEquals((releases + 2 + i) % 10, memoryManager.releasedArray.get(1).size());
//                }
//            } else if (OakMemoryManager.RELEASES_DEFAULT > 100){
//                assertEquals(releases + 2 + i, memoryManager.releasedArray.get(1).size());
//            }
//        }
//
//        int now = memoryManager.releasedArray.get(1).size();
//
//        for (int i = 0; i < items; i++) {
//            oak.put(i, i);
//        }
//
//        assertEquals(now, memoryManager.releasedArray.get(1).size());
//
//        for (Integer i = 0; i < items; i++) {
//            Integer value = (Integer) oak.get(i);
//            assertTrue(value != null);
//            assertEquals(i, value);
//        }
//
//        assertEquals(now, memoryManager.releasedArray.get(1).size());
//        oak.close();
//    }
//
//    @Test
//    public void compute(){
//        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
//                .setChunkMaxItems(maxItemsPerChunk)
//                .setChunkBytesPerItem(maxBytesPerChunkItem);
//        OakMap<Integer, Integer> oak = (OakMap<Integer, Integer>) builder.build();
//
//        Consumer<OakWBuffer> computer = new Consumer<OakWBuffer>() {
//            @Override
//            public void accept(OakWBuffer oakWBuffer) {
//                if (oakWBuffer.getInt(0) == 0) {
//                    oakWBuffer.putInt(0, 1);
//                }
//            }
//        };
//
//        Integer key = 0;
//        oak.put(key, key);
//        Integer value = oak.get(key);
//        assertTrue(value != null);
//        assertEquals(key, value);
//        oak.computeIfPresent(key, computer);
//        value = oak.get(key);
//        assertTrue(value != null);
//        assertEquals((Integer) 1, value);
//        oak.computeIfPresent(key, computer);
//        value = oak.get(key);
//        assertTrue(value != null);
//        assertEquals((Integer) 1, value);
//        oak.close();
//    }


}
