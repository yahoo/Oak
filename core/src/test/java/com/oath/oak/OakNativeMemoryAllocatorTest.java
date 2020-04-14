package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.oath.oak.common.OakCommonBuildersFactory;
import com.oath.oak.common.integer.OakIntSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OakNativeMemoryAllocatorTest {
    static final int valueSizeAfterSerialization = 4 * 1024 * 1024;
    static final int keysSizeAfterSerialization = Integer.BYTES;

    static int calcExpectedSize(int keyCount, int valueCount) {
        return (keyCount * keysSizeAfterSerialization) +
                (valueCount * (valueSizeAfterSerialization + valueOperator.getHeaderSize()));
    }

    private static final ValueUtilsImpl valueOperator = new ValueUtilsImpl();

    Slice allocate(OakNativeMemoryAllocator allocator, int size) {
        Slice s = new Slice();
        allocator.allocate(s, size, MemoryManager.Allocate.KEY);
        return s;
    }

    @Test
    public void allocateContention() throws InterruptedException {
        Random random = new Random();
        long capacity = 100;
        int blockSize = 8;
        int buffersPerBlock = 2;
        List<Block> blocks = Collections.synchronizedList(new ArrayList<>());
        int allocationSize = blockSize / buffersPerBlock;

        BlocksProvider mockProvider = mock(BlocksProvider.class);
        when(mockProvider.blockSize()).thenReturn(blockSize);
        when(mockProvider.getBlock()).thenAnswer(invocation -> {
            Thread.sleep(random.nextInt(500));
            Block newBlock = new Block(blockSize);
            blocks.add(newBlock);
            return newBlock;
        });
        OakNativeMemoryAllocator allocator = new OakNativeMemoryAllocator(capacity, mockProvider);

        int numAllocators = 10;
        ArrayList<Thread> threads = new ArrayList<>();

        for (int i = 0; i < numAllocators; i++) {
            Thread fn = new Thread(() -> allocate(allocator, allocationSize));
            threads.add(fn);
        }
        for (int i = 0; i < numAllocators; i++) {
            threads.get(i).start();
        }
        for (int i = 0; i < numAllocators; i++) {
            threads.get(i).join();
        }

        assertEquals(numAllocators * allocationSize, allocator.allocated());
        assertEquals(numAllocators / buffersPerBlock, blocks.size());
    }


    @Test
    public void checkCapacity() {

        int blockSize = BlocksPool.getInstance().blockSize();
        int capacity = blockSize * 3;
        OakNativeMemoryAllocator ma = new OakNativeMemoryAllocator(capacity);

        /* simple allocation */
        Slice bb = allocate(ma, 4);
        assertEquals(4, bb.getAllocatedLength());
        assertEquals(4, ma.getCurrentBlock().allocated());


        Slice bb1 = allocate(ma, 4);
        assertEquals(4, bb1.getAllocatedLength());
        assertEquals(8, ma.getCurrentBlock().allocated());

        Slice bb2 = allocate(ma, 8);
        assertEquals(8, bb2.getAllocatedLength());
        assertEquals(16, ma.getCurrentBlock().allocated());

        /* big allocation */
        Slice bb3 = allocate(ma, blockSize - 8);
        assertEquals(blockSize - 8,
                bb3.getAllocatedLength());                                   // check the new ByteBuffer size
        assertEquals(blockSize - 8,  // check the new block allocation
                ma.getCurrentBlock().allocated());

        /* complete up to full block allocation */
        Slice bb4 = allocate(ma, 8);
        assertEquals(8, bb4.getAllocatedLength());              // check the new ByteBuffer size
        assertEquals(blockSize,               // check the new block allocation
                ma.getCurrentBlock().allocated());

        /* next small allocation should move us to the next block */
        Slice bb5 = allocate(ma, 8);
        assertEquals(8, bb5.getAllocatedLength());           // check the newest ByteBuffer size
        assertEquals(8,                             // check the newest block allocation
                ma.getCurrentBlock().allocated());

        ma.close();
    }

    @Before
    public void init() {
        BlocksPool.setBlockSize(8 * 1024 * 1024);
    }

    @After
    public void tearDown() {
        BlocksPool.setBlockSize(BlocksPool.BLOCK_SIZE);
    }

    @Test
    public void checkOakCapacity() {
        int initBlocks = BlocksPool.getInstance().numOfRemainingBlocks();
        int blockSize = BlocksPool.getInstance().blockSize();
        int capacity = blockSize * 3;
        OakNativeMemoryAllocator ma = new OakNativeMemoryAllocator(capacity);
        int maxItemsPerChunk = 1024;

        // These will be updated on the fly
        int expectedEntryCount = 0;
        int expectedKeyCount = 0;
        int expectedValueCount = 0;

        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder()
            .setValueSerializer(new OakIntSerializer(valueSizeAfterSerialization))
            .setChunkMaxItems(maxItemsPerChunk)
            .setMemoryAllocator(ma);

        OakMap<Integer, Integer> oak = builder.build();
        expectedKeyCount += 1; // min key

        //check that before any allocation
        // (1) we have all the blocks in the pool except one which is in the allocator
        assertEquals(Math.max(0, initBlocks - 1), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the one block in the allocator
        assertEquals(ma.numOfAllocatedBlocks(), 1);

        // validate entry count
        assertEquals(expectedEntryCount, oak.entrySet().size());

        // validate allocation size
        assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), ma.allocated());   // check the newest block allocation

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
        assertEquals(Math.max(0, initBlocks - 1), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the one block in the allocator
        assertEquals(ma.numOfAllocatedBlocks(), 1);

        // validate entry count
        assertEquals(expectedEntryCount, oak.entrySet().size());

        assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), ma.allocated());   // check the newest block allocation

        // check that what you read is the same that you wrote
        Integer resultForKey = oak.firstKey();
        Integer resultForValue = oak.get(key);
        assertEquals(resultForKey, key);
        assertEquals(resultForValue, val);

        key = 1;
        oak.zc().put(key, val);
        expectedEntryCount += 1;
        expectedKeyCount += 1;
        expectedValueCount += 1;

        //check that after a double allocation of a block size
        // (1) we have all the blocks in the pool except two which are in the allocator
        assertEquals(Math.max(0, initBlocks - 2), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the two blocks in the allocator
        assertEquals(ma.numOfAllocatedBlocks(), 2);

        // validate entry count
        assertEquals(expectedEntryCount, oak.entrySet().size());

        // mind no addition of the size of integer key, as it was allocated in the previous block
        assertEquals(calcExpectedSize(0, 1), ma.getCurrentBlock().allocated());

        // check the newest block allocation
        assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), ma.allocated());   // check the total allocation

        // check that what you read is the same that you wrote
        resultForKey = oak.lastKey();
        resultForValue = oak.get(key);
        assertEquals(resultForKey, key);
        assertEquals(resultForValue, val);

        key = 2;
        oak.zc().put(key, val);
        expectedEntryCount += 1;
        expectedKeyCount += 1;
        expectedValueCount += 1;

        //check that after three allocations of a block size
        // (1) we have all the blocks in the pool except three which are in the allocator
        assertEquals(Math.max(0, initBlocks - 3), BlocksPool.getInstance().numOfRemainingBlocks());

        // (2) check the 3 blocks in the allocator
        assertEquals(ma.numOfAllocatedBlocks(), 3);

        // validate entry count
        assertEquals(expectedEntryCount, oak.entrySet().size());

        // mind no addition of the size of integer key, as it was allocated in the previous block
        assertEquals(calcExpectedSize(0, 1), ma.getCurrentBlock().allocated());

        // check the newest block allocation
        assertEquals(calcExpectedSize(expectedKeyCount, expectedValueCount), ma.allocated());   // check the total allocation

        // check that what you read is the same that you wrote
        resultForKey = oak.lastKey();
        resultForValue = oak.get(key);
        assertEquals(resultForKey, key);
        assertEquals(resultForValue, val);

        // we have set current OakMap capacity to be 3 block sizes,
        // thus we expect OakOutOfMemoryException
        key = 3;
        boolean gotException = false;
        try {
            oak.zc().put(key, val);
        } catch (OakOutOfMemoryException e) {
            gotException = true;
        }
        assertTrue(gotException);

        key = 0; // should be written
        Integer value = oak.get(key);
        assertEquals((Integer) 1, value);

        oak.zc().remove(key); // remove the key so we have space for more

        key = 3; // should not be written
        value = oak.get(key);
        assertNull(value);

        oak.zc().remove(1); // this should actually trigger the free of key 0 memory

        oak.close();
    }

    @Test
    public void checkFreelistOrdering() {
        long capacity = 100;
        OakNativeMemoryAllocator allocator = new OakNativeMemoryAllocator(capacity);
        allocator.collectStats();

        // Order is important here!
        int[] sizes = new int[]{4, 16, 8, 32};
        List<Slice> allocated = Arrays.stream(sizes)
                .mapToObj(curSize -> {
                    Slice s = new Slice();
                    allocator.allocate(s, curSize, MemoryManager.Allocate.KEY);
                    return s;
                }).collect(Collectors.toList());
        int bytesAllocated = IntStream.of(sizes).sum();

        allocated.forEach(allocator::free);

        OakNativeMemoryAllocator.Stats stats = allocator.getStats();
        assertEquals(sizes.length, stats.releasedBuffers);
        assertEquals(bytesAllocated, stats.releasedBytes);

        // Requesting a small buffer should not reclaim existing buffers
        allocate(allocator, 1);
        stats = allocator.getStats();
        assertEquals(0, stats.reclaimedBuffers);

        // Verify free list ordering
        Slice bb = allocate(allocator, 4);
        assertEquals(4, bb.getAllocatedLength());
        bb = allocate(allocator, 4);
        assertEquals(8, bb.getAllocatedLength());

        stats = allocator.getStats();
        assertEquals(2, stats.reclaimedBuffers);
        assertEquals(8, stats.reclaimedBytes);

        bb = allocate(allocator, 32);
        assertEquals(32, bb.getAllocatedLength());
        bb = allocate(allocator, 16);
        assertEquals(16, bb.getAllocatedLength());

        assertEquals(sizes.length, stats.reclaimedBuffers);
        // We lost 4 bytes recycling an 8-byte buffer for a 4-byte allocation
        assertEquals(bytesAllocated - 4, stats.reclaimedBytes);
    }
}
