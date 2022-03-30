/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import com.yahoo.oak.common.integer.OakIntSerializer;
import com.yahoo.oak.test_utils.ExecutorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NativeMemoryAllocatorTest {
    private static final int NUM_THREADS = 10;
    private static final long TIME_LIMIT_IN_SECONDS = 60;

    static final int VALUE_SIZE_AFTER_SERIALIZATION = 4 * 1024 * 1024;
    static final int KEYS_SIZE_AFTER_SERIALIZATION = Integer.BYTES;

    private ExecutorUtils<Slice> executor;

    NativeMemoryAllocator allocator;
    OakMap<Integer, Integer> oak;

    @Before
    public void setup() {
        executor = new ExecutorUtils<>(NUM_THREADS);
        BlocksPool.setBlockSize(8 * 1024 * 1024);
        allocator = null;
        oak = null;
    }

    @After
    public void finish() {
        executor.shutdownNow();
        if (oak != null) {
            oak.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        BlocksPool.setBlockSize(BlocksPool.DEFAULT_BLOCK_SIZE_BYTES);
        BlocksPool.clear();
    }

    static int calcExpectedSize(int keyCount, int valueCount) {
        return (keyCount * (KEYS_SIZE_AFTER_SERIALIZATION +     KEY_MEMORY_MANAGER.getHeaderSize())) +
                (valueCount * (VALUE_SIZE_AFTER_SERIALIZATION + VALUE_MEMORY_MANAGER.getHeaderSize()));
    }

    private static final MemoryManager VALUE_MEMORY_MANAGER = new SyncRecycleMemoryManager(null);
    private static final MemoryManager KEY_MEMORY_MANAGER = new NovaMemoryManager(null); //TODO maybe change later


    BlockAllocationSlice allocate(NativeMemoryAllocator allocator, int size) {
        BlockAllocationSlice s = (BlockAllocationSlice) VALUE_MEMORY_MANAGER.getEmptySlice();
        allocator.allocate(s, size);
        return s;
    }

    @Test
    public void allocateContention() throws ExecutorUtils.ExecutionError {
        Random random = new Random();
        long capacity = 100;
        int blockSize = 8;
        int buffersPerBlock = 2;
        List<Block> blocks = Collections.synchronizedList(new ArrayList<>());
        int allocationSize = blockSize / buffersPerBlock;

        BlocksProvider mockProvider = Mockito.mock(BlocksProvider.class);
        Mockito.when(mockProvider.blockSize()).thenReturn(blockSize);
        Mockito.when(mockProvider.getBlock()).thenAnswer(invocation -> {
            Thread.sleep(random.nextInt(500));
            Block newBlock = new Block(blockSize);
            blocks.add(newBlock);
            return newBlock;
        });
        allocator = new NativeMemoryAllocator(capacity, mockProvider);

        executor.submitTasks(NUM_THREADS, i -> () -> allocate(allocator, allocationSize));
        executor.shutdown(TIME_LIMIT_IN_SECONDS);

        Assert.assertEquals(NUM_THREADS * allocationSize, allocator.allocated());
        Assert.assertEquals(NUM_THREADS / buffersPerBlock, blocks.size());
    }


    @Test
    public void checkCapacity() {

        int blockSize = BlocksPool.getInstance().blockSize();
        int capacity = blockSize * 3;
        allocator = new NativeMemoryAllocator(capacity);

        /* simple allocation */
        BlockAllocationSlice bb = allocate(allocator, 4);
        Assert.assertEquals(4, bb.getAllocatedLength());
        Assert.assertEquals(4, allocator.getCurrentBlock().allocatedWithPossibleDelta());


        BlockAllocationSlice bb1 = allocate(allocator, 4);
        Assert.assertEquals(4, bb1.getAllocatedLength());
        Assert.assertEquals(8, allocator.getCurrentBlock().allocatedWithPossibleDelta());

        BlockAllocationSlice bb2 = allocate(allocator, 8);
        Assert.assertEquals(8, bb2.getAllocatedLength());
        Assert.assertEquals(16, allocator.getCurrentBlock().allocatedWithPossibleDelta());

        /* big allocation */
        BlockAllocationSlice bb3 = allocate(allocator, blockSize - 8);
        Assert.assertEquals(blockSize - 8,
                bb3.getAllocatedLength());                                   // check the new ByteBuffer size
        Assert.assertEquals(blockSize - 8,  // check the new block allocation
                allocator.getCurrentBlock().allocatedWithPossibleDelta());

        /* complete up to full block allocation */
        BlockAllocationSlice bb4 = allocate(allocator, 8);
        Assert.assertEquals(8, bb4.getAllocatedLength());              // check the new ByteBuffer size
        Assert.assertEquals(blockSize,               // check the new block allocation
                allocator.getCurrentBlock().allocatedWithPossibleDelta());

        /* next small allocation should move us to the next block */
        BlockAllocationSlice bb5 = allocate(allocator, 8);
        Assert.assertEquals(8, bb5.getAllocatedLength());           // check the newest ByteBuffer size
        Assert.assertEquals(8,                             // check the newest block allocation
                allocator.getCurrentBlock().allocatedWithPossibleDelta());
    }

    @Test
    public void checkOakCapacity() {
        int initBlocks = BlocksPool.getInstance().numOfRemainingBlocks();
        int blockSize = BlocksPool.getInstance().blockSize();
        int capacity = blockSize * 3;

        int maxItemsPerChunk = 1024;

        // These will be updated on the fly
        int expectedEntryCount = 0;
        int expectedKeyCount = 0;
        int expectedValueCount = 0;

        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
                .setValueSerializer(new OakIntSerializer(VALUE_SIZE_AFTER_SERIALIZATION))
                .setChunkMaxItems(maxItemsPerChunk)
                .setMemoryCapacity(capacity);
        oak = builder.buildOrderedMap();
        allocator = (NativeMemoryAllocator) oak.getValuesMemoryManager().getBlockMemoryAllocator();

        expectedKeyCount += 1; // min key

        //check that before any allocation
        // (1) we have all the blocks in the pool except one which is in the allocator
        Assert.assertEquals(Math.max(0, initBlocks - 1), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the one block in the allocator
        Assert.assertEquals(allocator.numOfAllocatedBlocks(), 1);

        // validate entry count
        Assert.assertEquals(expectedEntryCount, oak.entrySet().size());

        // validate allocation size
        // check the newest block allocation
        Assert.assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), allocator.allocated());

        Integer val = 1;
        Integer key = 0;

        // pay attention that the given value serializer CheckOakCapacityValueSerializer
        // will transform a single integer into huge buffer of size about 100MB,
        // what is currently one block size
        oak.zc().put(key, val);
        expectedEntryCount += 1;
        expectedKeyCount += 1;
        expectedValueCount += 1;

        //check that after a single allocation of a block size
        // (1) we have all the blocks in the pool except one which is in the allocator
        Assert.assertEquals(Math.max(0, initBlocks - 1), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the one block in the allocator
        Assert.assertEquals(allocator.numOfAllocatedBlocks(), 1);

        // validate entry count
        Assert.assertEquals(expectedEntryCount, oak.entrySet().size());

        // check the newest block allocation
        Assert.assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), allocator.allocated());

        // check that what you read is the same that you wrote
        Integer resultForKey = oak.firstKey();
        Integer resultForValue = oak.get(key);
        Assert.assertEquals(resultForKey, key);
        Assert.assertEquals(resultForValue, val);

        key = 1;
        oak.zc().put(key, val);
        expectedEntryCount += 1;
        expectedKeyCount += 1;
        expectedValueCount += 1;

        //check that after a double allocation of a block size
        // (1) we have all the blocks in the pool except two which are in the allocator
        Assert.assertEquals(Math.max(0, initBlocks - 2), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the two blocks in the allocator
        Assert.assertEquals(allocator.numOfAllocatedBlocks(), 2);

        // validate entry count
        Assert.assertEquals(expectedEntryCount, oak.entrySet().size());

        // mind no addition of the size of integer key, as it was allocated in the previous block
        Assert.assertEquals(calcExpectedSize(0, 1), allocator.getCurrentBlock().allocatedWithPossibleDelta());

        // check the newest block allocation
        // check the total allocation
        Assert.assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), allocator.allocated());

        // check that what you read is the same that you wrote
        resultForKey = oak.lastKey();
        resultForValue = oak.get(key);
        Assert.assertEquals(resultForKey, key);
        Assert.assertEquals(resultForValue, val);

        key = 2;
        oak.zc().put(key, val);
        expectedEntryCount += 1;
        expectedKeyCount += 1;
        expectedValueCount += 1;

        //check that after three allocations of a block size
        // (1) we have all the blocks in the pool except three which are in the allocator
        Assert.assertEquals(Math.max(0, initBlocks - 3), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the 3 blocks in the allocator
        Assert.assertEquals(allocator.numOfAllocatedBlocks(), 3);

        // validate entry count
        Assert.assertEquals(expectedEntryCount, oak.entrySet().size());

        // mind no addition of the size of integer key, as it was allocated in the previous block
        Assert.assertEquals(calcExpectedSize(0, 1), allocator.getCurrentBlock().allocatedWithPossibleDelta());

        // check the newest block allocation
        // check the total allocation
        Assert.assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), allocator.allocated());

        // check that what you read is the same that you wrote
        resultForKey = oak.lastKey();
        resultForValue = oak.get(key);
        Assert.assertEquals(resultForKey, key);
        Assert.assertEquals(resultForValue, val);

        // we have set current OakMap capacity to be 3 block sizes,
        // thus we expect OakOutOfMemoryException
        key = 3;
        boolean gotException = false;
        try {
            oak.zc().put(key, val);
        } catch (OakOutOfMemoryException e) {
            gotException = true;
        }
        Assert.assertTrue(gotException);

        key = 0; // should be written
        Integer value = oak.get(key);
        Assert.assertEquals((Integer) 1, value);

        oak.zc().remove(key); // remove the key so we have space for more

        key = 3; // should not be written
        value = oak.get(key);
        Assert.assertNull(value);

        oak.zc().remove(1); // this should actually trigger the free of key 0 memory
    }

    @Test
    public void checkFreelistOrdering() {
        long capacity = 100;
        allocator = new NativeMemoryAllocator(capacity);
        allocator.collectStats();

        // Order is important here!
        int[] sizes = new int[]{4 + VALUE_MEMORY_MANAGER.getHeaderSize(),
            16 + VALUE_MEMORY_MANAGER.getHeaderSize(),
            8 + VALUE_MEMORY_MANAGER.getHeaderSize(),
            32 + + VALUE_MEMORY_MANAGER.getHeaderSize()};
        List<Slice> allocated = Arrays.stream(sizes)
                .mapToObj(curSize -> {
                    Slice s = VALUE_MEMORY_MANAGER.getEmptySlice();
                    allocator.allocate(s, curSize);
                    return s;
                }).collect(Collectors.toList());
        int bytesAllocated = IntStream.of(sizes).sum();

        allocated.forEach(allocator::free);

        NativeMemoryAllocator.Stats stats = allocator.getStats();
        Assert.assertEquals(sizes.length, stats.releasedBuffers);
        Assert.assertEquals(bytesAllocated, stats.releasedBytes);

        // Requesting a small buffer should not reclaim existing buffers
        allocate(allocator, 1);
        stats = allocator.getStats();
        Assert.assertEquals(0, stats.reclaimedBuffers);

        // Verify free list ordering
        BlockAllocationSlice bb = allocate(allocator, 4);
        Assert.assertEquals(4, ((BlockAllocationSlice) bb).getAllocatedLength());
        bb = allocate(allocator, 4);
        Assert.assertEquals(4, ((BlockAllocationSlice) bb).getAllocatedLength());

        stats = allocator.getStats();
        Assert.assertEquals(0, stats.reclaimedBuffers);
        Assert.assertEquals(0, stats.reclaimedBytes);

        bb = allocate(allocator, 32);
        Assert.assertEquals(44, bb.getAllocatedLength());
        bb = allocate(allocator, 16);
        Assert.assertEquals(16, bb.getAllocatedLength());

        //Assert.assertEquals(sizes.length, stats.reclaimedBuffers);
        // We lost 4 bytes recycling an 8-byte buffer for a 4-byte allocation
        //Assert.assertEquals(bytesAllocated - 4, stats.reclaimedBytes);
    }
}
