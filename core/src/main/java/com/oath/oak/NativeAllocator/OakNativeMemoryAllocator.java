/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak.NativeAllocator;

import com.oath.oak.OakMemoryAllocator;
import com.oath.oak.OakOutOfMemoryException;
import com.oath.oak.Slice;
import com.oath.oak.ThreadIndexCalculator;

import java.nio.ByteBuffer;
import java.util.AbstractMap;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OakNativeMemoryAllocator implements OakMemoryAllocator {

    // TODO: Why hold slice?
    private static class FreeChuck {
        long id;
        long length;
        Slice slice;

        FreeChuck(long id, long length, Slice slice) {
            this.id = id;
            this.length = length;
            this.slice = slice;
        }
    }

    // When allocating n bytes and there are buffers in the free list, only free buffers of size <= n * RECLAIM_FACTOR will be recycled
    // This parameter may be tuned for performance vs off-heap memory utilization
    private static final int RECLAIM_FACTOR = 2;
    public static final int INVALID_BLOCK_ID = 0;

    // mapping IDs to blocks allocated solely to this Allocator
    private final Block[] blocksArray;
    private final AtomicInteger idGenerator = new AtomicInteger(1);

    // free list of Slices which can be reused - sorted by buffer size, then by unique hash
    private final ConcurrentSkipListSet<FreeChuck> freeList = new ConcurrentSkipListSet<>((x, y) -> {
        if (x.length == y.length) return (int) (x.id - y.id);
        return (int) (x.length - y.length);
    });
    private final FreeChuck[] dummies = new FreeChuck[ThreadIndexCalculator.MAX_THREADS];
    private final ThreadIndexCalculator threadIndexCalculator = ThreadIndexCalculator.newInstance();

    private final BlocksProvider blocksProvider;
    private Block currentBlock;

    // the memory allocation limit for this Allocator
    // current capacity is set as number of blocks (!) allocated for this OakMap
    // can be changed to check only according to real allocation (allocated field)
    private final long capacity;

    // number of bytes allocated for this Oak among different Blocks
    // can be calculated, but kept for easy access
    private final AtomicLong allocated = new AtomicLong(0);
    private final AtomicLong freeCounter = new AtomicLong(0);

    // flag allowing not to close the same allocator twice
    private AtomicBoolean closed = new AtomicBoolean(false);

    // constructor
    // input param: memory capacity given to this Oak. Uses default BlocksPool
    public OakNativeMemoryAllocator(long capacity) {
        this(capacity, BlocksPool.getInstance());
    }

    // A testable constructor
    OakNativeMemoryAllocator(long capacity, BlocksProvider blocksProvider) {
        this.blocksProvider = blocksProvider;
        int blockArraySize = ((int) (capacity / blocksProvider.blockSize())) + 1;
        // first entry of blocksArray is always empty
        this.blocksArray = new Block[blockArraySize + 1];
        // initially allocate one single block from pool
        // this may lazy initialize the pool and take time if this is the first call for the pool
        allocateNewCurrentBlock();
        this.capacity = capacity;
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            dummies[i] = new FreeChuck(-1, 0, null);
        }
    }

    @Override
    public ByteBuffer allocate(int size) {
        return allocateSlice(size).getByteBuffer();
    }

    // Allocates ByteBuffer of the given size, either from freeList or (if it is still possible)
    // within current block bounds.
    // Otherwise new block is allocated within Oak memory bounds. Thread safe.
    public Slice allocateSlice(int size) {

        FreeChuck myDummy = dummies[threadIndexCalculator.getIndex()];
        while (!freeList.isEmpty()) {
            myDummy.length = size;
            FreeChuck bestFit = freeList.higher(myDummy);
            if (bestFit == null) break;
            if (bestFit.slice.getByteBuffer().remaining() > (RECLAIM_FACTOR * size))
                break;     // all remaining buffers are too big
            if (freeList.remove(bestFit)) {
                if (stats != null)
                    stats.reclaim(size);
                return bestFit.slice;
            }
        }

        Slice s = null;
        // freeList is empty or there is no suitable slice
        while (s == null) {
            try {
                s = currentBlock.allocate(size);
            } catch (OakOutOfMemoryException e) {
                // there is no space in current block
                // may be a buffer bigger than any block is requested?
                if (size > blocksProvider.blockSize()) {
                    throw new OakOutOfMemoryException();
                }
                // does allocation of new block brings us out of capacity?
                if ((numberOfBocks() + 1) * blocksProvider.blockSize() > capacity) {
                    throw new OakOutOfMemoryException();
                } else {
                    // going to allocate additional block (big chunk of memory)
                    // need to be thread-safe, so not many blocks are allocated
                    // locking is actually the most reasonable way of synchronization here
                    synchronized (this) {
                        if (currentBlock.allocated() + size > currentBlock.getCapacity()) {
                            allocateNewCurrentBlock();
                        }
                    }
                }
            }
        }
        allocated.addAndGet(size);
        return s;
    }

    // Releases memory (makes it available for reuse) without other GC consideration.
    // Meaning this request should come while it is ensured none is using this memory.
    // Thread safe.
    // IMPORTANT: it is assumed free will get ByteBuffers only initially allocated from this
    // Allocator!
    @Override
    public void free(ByteBuffer bb) {
        allocated.addAndGet(-(bb.remaining()));
        if (stats != null) stats.release(bb);
        freeList.add(new FreeChuck(freeCounter.getAndIncrement(), bb.remaining(), new Slice(INVALID_BLOCK_ID, bb)));
    }

    public void freeSlice(Slice slice) {
        allocated.addAndGet(-(slice.getByteBuffer().remaining()));
        if (stats != null) stats.release(slice.getByteBuffer());
        freeList.add(new FreeChuck(freeCounter.getAndIncrement(), slice.getByteBuffer().remaining(), slice));
    }

    // Releases all memory allocated for this Oak (should be used as part of the Oak destruction)
    // Not thread safe, should be a single thread call. (?)
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (int i = 1; i <= numberOfBocks(); i++) {
            blocksProvider.returnBlock(blocksArray[i]);
        }
        // no need to do anything with the free list,
        // as all free list members were residing on one of the (already released) blocks
    }

    // Returns the off-heap allocation of this OakMap
    @Override
    public long allocated() {
        return allocated.get();
    }

    // When some buffer need to be read from a random block
    public ByteBuffer readByteBufferFromBlockID(
            Integer blockID, int bufferPosition, int bufferLength) {
        Block b = blocksArray[blockID];
        return b.getReadOnlyBufferForThread(bufferPosition, bufferLength);
    }

    // used only for testing
    Block getCurrentBlock() {
        return currentBlock;
    }

    // used only for testing
    int numOfAllocatedBlocks() {
        return (int) numberOfBocks();
    }

    // This method MUST be called within a thread safe context !!!
    private void allocateNewCurrentBlock() {
        Block b = blocksProvider.getBlock();
        int blockID = idGenerator.getAndIncrement();
        this.blocksArray[blockID] = b;
        b.setID(blockID);
        this.currentBlock = b;
    }

    private long numberOfBocks() {
        return idGenerator.get() - 1;
    }

    private Stats stats = null;

    public void collectStats() {
        stats = new Stats();
    }

    public Stats getStats() {
        return stats;
    }

    public static class Stats {
        public int reclaimedBuffers;
        public int releasedBuffers;
        public long releasedBytes;
        public long reclaimedBytes;

        public void release(ByteBuffer bb) {
            synchronized (this) {
                releasedBuffers++;
                releasedBytes += bb.remaining();
            }
        }

        public void reclaim(int size) {
            synchronized (this) {
                reclaimedBuffers++;
                reclaimedBytes += size;
            }
        }
    }

}


