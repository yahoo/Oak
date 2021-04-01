/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class NativeMemoryAllocator implements BlockMemoryAllocator {

    // When allocating n bytes and there are buffers in the free list, only free buffers of size <= n *
    // REUSE_MAX_MULTIPLIER will be recycled
    // This parameter may be tuned for performance vs off-heap memory utilization
    private static final int REUSE_MAX_MULTIPLIER = 2;
    public static final int INVALID_BLOCK_ID = 0;

    // mapping IDs to blocks allocated solely to this Allocator
    private Block[] blocksArray;
    private final AtomicInteger idGenerator = new AtomicInteger(1);

    /**
     * free list of Slices which can be reused.
     * They are sorted by the slice length, then by the block id, then by their offset.
     * See {@code Slice.compareTo(Slice)} for more information.
     */
    private final ConcurrentSkipListSet<Slice> freeList = new ConcurrentSkipListSet<>();

    private final BlocksProvider blocksProvider;
    private Block currentBlock;

    // the memory allocation limit for this Allocator
    // current capacity is set as number of blocks (!) allocated for this OakMap
    // can be changed to check only according to real allocation (allocated field)
    private final long capacity;

    // number of bytes allocated for this Oak among different Blocks
    // can be calculated, but kept for easy access
    private final AtomicLong allocated = new AtomicLong(0);

    // flag allowing not to close the same allocator twice
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // constructor
    // input param: memory capacity given to this Oak. Uses default BlocksPool
    NativeMemoryAllocator(long capacity) {
        this(capacity, BlocksPool.getInstance());
    }

    // A testable constructor
    NativeMemoryAllocator(long capacity, BlocksProvider blocksProvider) {
        this.blocksProvider = blocksProvider;
        int blockArraySize = ((int) (capacity / blocksProvider.blockSize())) + 1;
        // first entry of blocksArray is always empty
        this.blocksArray = new Block[blockArraySize + 1];
        // initially allocate one single block from pool
        // this may lazy initialize the pool and take time if this is the first call for the pool
        allocateNewCurrentBlock();
        this.capacity = capacity;
    }

    // Allocates an off-heap cut of the given size, either from freeList or (if it is still possible)
    // within current block bounds.
    // Otherwise, new block is allocated within Oak memory bounds. Thread safe.
    // Given size already includes the size for metadata header if needed.
    @Override
    public boolean allocate(Slice s, int size) {
        // While the free list is not empty there can be a suitable free slice to reuse.
        // To search a free slice, we use the input slice as a dummy and change its length to the desired length.
        // Then, we use freeList.higher(s) which returns a free slice with greater or equal length to the length of the
        // dummy with time complexity of O(log N), where N is the number of free slices.
        while (!freeList.isEmpty()) {
            s.initializeLookupDummy(size);
            Slice bestFit = freeList.higher(s);
            if (bestFit == null) {
                break;
            }
            // If the best fit is more than REUSE_MAX_MULTIPLIER times as big than the desired length, than a new
            // buffer is allocated instead of reusing.
            // This means that currently buffers are not split, so there is some internal fragmentation.
            if (bestFit.getAllocatedLength() > (REUSE_MAX_MULTIPLIER * size)) {
                break;     // all remaining buffers are too big
            }
            // If multiple threads got the same bestFit only one can use it (the one which succeeds in removing it
            // from the free list).
            // The rest restart the while loop.
            if (freeList.remove(bestFit)) {
                if (stats != null) {
                    stats.reclaim(size);
                }
                s.copyAllocationInfoFrom(bestFit);
                return true;
            }
        }

        boolean isAllocated = false;
        // freeList is empty or there is no suitable slice
        while (!isAllocated) {
            try {
                // The ByteBuffer inside this slice is the thread's ByteBuffer
                isAllocated = currentBlock.allocate(s, size);
            } catch (OakOutOfMemoryException e) {
                // there is no space in current block
                // may be a buffer bigger than any block is requested?
                if (size > blocksProvider.blockSize()) {
                    throw new IllegalArgumentException(
                            String.format("Cannot allocate larger items than the block size (block size: %s).",
                                    blocksProvider.blockSize()));
                }
                // does allocation of new block brings us out of capacity?
                if ((numberOfBlocks() + 1) * blocksProvider.blockSize() > capacity) {
                    throw new OakOutOfMemoryException(
                            String.format("This allocator capacity was exceeded (capacity: %s).", capacity));
                } else {
                    // going to allocate additional block (big chunk of memory)
                    // need to be thread-safe, so not many blocks are allocated
                    // locking is actually the most reasonable way of synchronization here
                    synchronized (this) {
                        if (currentBlock.allocatedWithPossibleDelta() + size > currentBlock.getCapacity()) {
                            allocateNewCurrentBlock();
                        }
                    }
                }
            }
        }
        allocated.addAndGet(size);
        return true;
    }

    // Releases memory (makes it available for reuse) without other GC consideration.
    // Meaning this request should come while it is ensured none is using this memory.
    // Thread safe.
    // IMPORTANT: it is assumed free will get an allocation only initially allocated from this
    // Allocator!
    @Override
    public void free(Slice s) {
        int size = s.getAllocatedLength();
        allocated.addAndGet(-size);
        if (stats != null) {
            stats.release(size);
        }
        freeList.add(s.getDuplicatedSlice());
    }

    // Releases all memory allocated for this Oak (should be used as part of the Oak destruction)
    // Not thread safe, should be a single thread call. (?)
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        // Release the hold of the block array and return it the provider.
        Block[] b = blocksArray;
        blocksArray = null;

        // Reset "closed" to apply a memory barrier before actually returning the block.
        closed.set(true);

        for (int i = 1; i <= numberOfBlocks(); i++) {
            blocksProvider.returnBlock(b[i]);
        }
        // no need to do anything with the free list,
        // as all free list members were residing on one of the (already released) blocks
    }

    // Returns the off-heap allocation of this OakMap
    @Override
    public long allocated() {
        return allocated.get();
    }

    public int getFreeListLength() {
        return freeList.size();
    }


    @Override
    public boolean isClosed() {
        return closed.get();
    }

    // When some buffer need to be read from a random block
    @Override
    public void readByteBuffer(Slice s) {
        int blockID = s.getAllocatedBlockID();
        // Validates that the input block id is valid.
        // This check should be automatically eliminated by the compiler in production.
        assert blockID > NativeMemoryAllocator.INVALID_BLOCK_ID :
                String.format("Invalid block-id: %s", s);
        Block b = blocksArray[blockID];
        s.setBuffer(b.getBuffer());
    }

    // used only for testing
    Block getCurrentBlock() {
        return currentBlock;
    }

    // used only for testing
    int numOfAllocatedBlocks() {
        return (int) numberOfBlocks();
    }

    // This method MUST be called within a thread safe context !!!
    private void allocateNewCurrentBlock() {
        Block b = blocksProvider.getBlock();
        int blockID = idGenerator.getAndIncrement();
        this.blocksArray[blockID] = b;
        b.setID(blockID);
        this.currentBlock = b;
    }

    private long numberOfBlocks() {
        return idGenerator.get() - 1;
    }

    private Stats stats = null;

    public void collectStats() {
        stats = new Stats();
    }

    public Stats getStats() {
        return stats;
    }

    static class Stats {
        int reclaimedBuffers;
        int releasedBuffers;
        long releasedBytes;
        long reclaimedBytes;

        public void release(int size) {
            synchronized (this) {
                releasedBuffers++;
                releasedBytes += size;
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


