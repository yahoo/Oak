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
import java.util.logging.Logger;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class OakMemoryManagerTest {

    Logger log = Logger.getLogger(OakMemoryManagerTest.class.getName());
    private MemoryManager memoryManager;
    private static int maxItemsPerChunk = 1024;
    private static int maxBytesPerChunkItem = 100;
    private static int valueSizeAfterSerialization = Integer.MAX_VALUE/40;
    private static int keyBufferSize = maxItemsPerChunk*maxBytesPerChunkItem;
    public static class CheckOakCapacityValueSerializer implements OakSerializer<Integer> {

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
            return valueSizeAfterSerialization;
        }
    }

    @Before
    public void init() {
//        memoryManager = new // one OakMap capacity about KB, 3 blocks
//            MemoryManager(BlocksPool.BLOCK_SIZE*3,null);
    }

    @After
    public void finish() {
//        memoryManager.close();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkCapacity() {

        memoryManager = new // one OakMap capacity about KB, 3 blocks
            MemoryManager(BlocksPool.BLOCK_SIZE*3,null);

        /* simple allocation */
        ByteBuffer bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());        // check the new ByteBuffer size
        assertEquals(4,                         // check block allocation
            ((OakNativeMemoryAllocator)memoryManager.getMemoryAllocator())
                .getCurrentBlock().allocated());

        ByteBuffer bb1 = memoryManager.allocate(4);
        assertEquals(4, bb1.remaining());       // check the new ByteBuffer size
        assertEquals(8,                         // check block allocation
            ((OakNativeMemoryAllocator)memoryManager.getMemoryAllocator())
                .getCurrentBlock().allocated());

        ByteBuffer bb2 = memoryManager.allocate(8);
        assertEquals(8, bb2.remaining());       // check the new ByteBuffer size
        assertEquals(16,                        // check block allocation
            ((OakNativeMemoryAllocator)memoryManager.getMemoryAllocator())
                .getCurrentBlock().allocated());

        /* big allocation */
        ByteBuffer bb3 = memoryManager.allocate(BlocksPool.BLOCK_SIZE - 8);
        assertEquals(BlocksPool.BLOCK_SIZE - 8,
            bb3.remaining());                                   // check the new ByteBuffer size
        assertEquals(BlocksPool.BLOCK_SIZE - 8,  // check the new block allocation
            ((OakNativeMemoryAllocator)memoryManager.getMemoryAllocator())
                .getCurrentBlock().allocated());

        /* complete up to full block allocation */
        ByteBuffer bb4 = memoryManager.allocate(8);
        assertEquals(8, bb4.remaining());              // check the new ByteBuffer size
        assertEquals(BlocksPool.BLOCK_SIZE,               // check the new block allocation
            ((OakNativeMemoryAllocator)memoryManager.getMemoryAllocator())
                .getCurrentBlock().allocated());

        /* next small allocation should move us to the next block */
        ByteBuffer bb5 = memoryManager.allocate(8);
        assertEquals(8, bb5.remaining());           // check the newest ByteBuffer size
        assertEquals(8,                             // check the newest block allocation
            ((OakNativeMemoryAllocator)memoryManager.getMemoryAllocator())
                .getCurrentBlock().allocated());

        memoryManager.close();
    }

    @Test
    public void checkOakCapacity() {

        OakMapBuilder builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem)
                .setValueSerializer(new CheckOakCapacityValueSerializer())
                .setMemoryCapacity(BlocksPool.BLOCK_SIZE*3);

        OakMap<Integer, Integer> oak = (OakMap<Integer, Integer>) builder.build();

        //check that before any allocation
        // (1) we have all the blocks in the pool except one which is in the allocator
        assertEquals(BlocksPool.getInstance().numOfRemainingBlocks(),
            BlocksPool.NUMBER_OF_BLOCKS-1);
        // (2) check the one block in the allocator
        assertEquals(
            ((OakNativeMemoryAllocator)oak.getMemoryManager().getMemoryAllocator()).
                numOfAllocatedBlocks(),
            1);

        Integer val = 1;
        Integer key = 0;

        // pay attention that the given value serializer CheckOakCapacityValueSerializer
        // will transform a single integer into huge buffer of size about 100MB,
        // what is currently one block size
        oak.put(key, val);

        //check that after a single allocation of a block size
        // (1) we have all the blocks in the pool except one which is in the allocator
        assertEquals(BlocksPool.getInstance().numOfRemainingBlocks(),
            BlocksPool.NUMBER_OF_BLOCKS-1);
        // (2) check the one block in the allocator
        assertEquals(
            ((OakNativeMemoryAllocator)oak.getMemoryManager().getMemoryAllocator()).
                numOfAllocatedBlocks(),
            1);
        assertEquals(valueSizeAfterSerialization + keyBufferSize,    // check the newest block allocation
            ((OakNativeMemoryAllocator)oak.getMemoryManager().getMemoryAllocator())
                .getCurrentBlock().allocated());
        // check that what you read is the same that you wrote
        Integer resultForKey = oak.getMinKey();
        Integer resultForValue = oak.get(key);
        assertEquals(resultForKey,key);
        assertEquals(resultForValue,val);

        key = 1;
        oak.put(key, val);

        //check that after a double allocation of a block size
        // (1) we have all the blocks in the pool except two which are in the allocator
        assertEquals(BlocksPool.getInstance().numOfRemainingBlocks(),
            BlocksPool.NUMBER_OF_BLOCKS-2);
        // (2) check the two blocks in the allocator
        assertEquals(
            ((OakNativeMemoryAllocator)oak.getMemoryManager().getMemoryAllocator()).
                numOfAllocatedBlocks(),
            2);
        assertEquals(valueSizeAfterSerialization,    // check the newest block allocation
            ((OakNativeMemoryAllocator)oak.getMemoryManager().getMemoryAllocator())
                .getCurrentBlock().allocated());
        assertEquals(valueSizeAfterSerialization*2 + keyBufferSize,    // check the total allocation
            oak.getMemoryManager().getMemoryAllocator().allocated());
        // check that what you read is the same that you wrote
        resultForKey = oak.getMaxKey();
        resultForValue = oak.get(key);
        assertEquals(resultForKey,key);
        assertEquals(resultForValue,val);

        key = 2;
        oak.put(key, val);

        //check that after three allocations of a block size
        // (1) we have all the blocks in the pool except three which are in the allocator
        assertEquals(BlocksPool.getInstance().numOfRemainingBlocks(),
            BlocksPool.NUMBER_OF_BLOCKS-3);
        // (2) check the 3 blocks in the allocator
        assertEquals(
            ((OakNativeMemoryAllocator)oak.getMemoryManager().getMemoryAllocator()).
                numOfAllocatedBlocks(),
            3);
        assertEquals(valueSizeAfterSerialization,    // check the newest block allocation
            ((OakNativeMemoryAllocator)oak.getMemoryManager().getMemoryAllocator())
                .getCurrentBlock().allocated());
        assertEquals(valueSizeAfterSerialization*3 + keyBufferSize,    // check the total allocation
            oak.getMemoryManager().getMemoryAllocator().allocated());
        // check that what you read is the same that you wrote
        resultForKey = oak.getMaxKey();
        resultForValue = oak.get(key);
        assertEquals(resultForKey,key);
        assertEquals(resultForValue,val);

        // we have set current OakMap capacity to be 3 block sizes,
        // thus we expect OakOutOfMemoryException
        key = 3;
        boolean gotException = false;
        try {
            oak.put(key, val);
        } catch (OakOutOfMemoryException e) {
            gotException = true;
        }
        assertTrue(gotException);

        key = 0; // should be written
        Integer value = (Integer) oak.get(key);
        assertTrue(value != null);
        assertEquals((Integer) 1, value);

        // request one release so GC can be triggered
        oak.getMemoryManager().setGCtrigger(1);
        oak.remove(key); // remove the key so we have space for more

        key = 3; // should not be written
        value = (Integer) oak.get(key);
        assertTrue(value == null);

        oak.remove(1); // this should actually trigger the free of key 0 memory

        for(int i = 0; i < 10; i++){
            key = i;
            oak.put(key, val);
            oak.remove(key);
        }

        oak.close();
    }



    @Test
    public void checkRelease() {

        memoryManager = new // one OakMap capacity about KB, 3 blocks
            MemoryManager(BlocksPool.BLOCK_SIZE*3,null);

        memoryManager.setGCtrigger(10); // trigger release after releasing 10 byte buffers

        ByteBuffer bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(4, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(8, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(12, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(16, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(20, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(24, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(28, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(32, memoryManager.allocated());
        memoryManager.release(bb);

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(36, memoryManager.allocated());
        memoryManager.release(bb);

        assertEquals(9, memoryManager.releasedArray.get(1).size());

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(40, memoryManager.allocated());
        memoryManager.release(bb);

        assertEquals(0, memoryManager.releasedArray.get(1).size());

        memoryManager.close();
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
