/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

class OakNativeMemoryAllocator implements OakMemoryAllocator{

    // blocks allocated solely to this Allocator
    private final ConcurrentLinkedQueue<Block> blocks = new ConcurrentLinkedQueue<Block>();
    // free list of ByteBuffers which can be reused
    private final ConcurrentSkipListSet<Pair<Integer,ByteBuffer>> freeList =
        new ConcurrentSkipListSet<>(Comparator.comparing(Pair::getKey));
    private Block currentBlock;

    // this boolean doesn't allow memory to be reused, by default set to false
    // but can be set to true in testing (or manually configured for some special run)
    private boolean stopMemoryReuse = false;

    // the memory allocation limit for this Allocator
    // current capacity is set as number of blocks (!) allocated for this OakMap
    // can be changed to check only according to real allocation (allocated field)
    private final long capacity;

    // number of bytes allocated for this Oak among different Blocks
    // can be calculated, but kept for easy access
    private final AtomicLong allocated = new AtomicLong(0);

    private final BlocksPool pool;

    // constructor
    // input param: memory capacity given to this Oak
    OakNativeMemoryAllocator(long capacity) {
        this(capacity, null);
    }

    OakNativeMemoryAllocator(long capacity, BlocksPool pool) {
        // initially allocate one single block from pool
        // this may lazy initialize the pool and take time if this is the first call for the pool
        if (pool == null) {
            this.pool = new BlocksPool();
            this.pool.registerAllocator();
        } else {
            this.pool = pool;
            this.pool.registerAllocator();
        }

        Block b = this.pool.getBlock();
        this.blocks.add(b);
        this.currentBlock = b;
        this.capacity = capacity;
    }

    // Allocates ByteBuffer of the given size, either from freeList or (if it is still possible)
    // within current block bounds.
    // Otherwise new block is allocated within Oak memory bounds. Thread safe.
    @Override
    public ByteBuffer allocate(int size) {

        if (!stopMemoryReuse && !freeList.isEmpty()) {
            for (Pair<Integer, ByteBuffer> kv : freeList) {
                ByteBuffer bb = kv.getValue();
                if (bb.capacity() >= size && freeList.remove(kv)) {
                    assert bb.position() == 0;
                    return bb;
                }
            }
        }

        ByteBuffer bb = null;
        // freeList is empty or there is no suitable slice
        while (bb == null) {
            try {
                bb = currentBlock.allocate(size);
            } catch (OakOutOfMemoryException e) {
                // there is no space in current block
                // may be a buffer bigger than any block is requested?
                if (size > BlocksPool.BLOCK_SIZE) {
                    throw new OakOutOfMemoryException();
                }
                // does allocation of new block brings us out of capacity?
                if ((blocks.size() + 1) * BlocksPool.BLOCK_SIZE > capacity) {
                    throw new OakOutOfMemoryException();
                } else {
                    Block b = pool.getBlock();
                    this.blocks.add(b);
                    this.currentBlock = b;
                }
            }
        }
        allocated.addAndGet(size);
        assert bb.position() == 0;
        return bb;
    }

    // Releases memory (makes it available for reuse) without other GC consideration.
    // Meaning this request should come while it is ensured none is using this memory.
    // Thread safe.
    // IMPORTANT: it is assumed free will get ByteBuffers only initially allocated from this
    // Allocator!
    @Override
    public void free(ByteBuffer bb) {
        allocated.addAndGet(-(bb.limit()));
        bb.clear();
        // ZERO THE MEMORY: A new byte array will automatically be initialized with all zeroes
        byte[] zeroes = new byte[bb.remaining()];
        bb.put(zeroes);
        bb.rewind(); // put the position back to zero
        freeList.add(new Pair<Integer, ByteBuffer>(System.identityHashCode(bb),bb));
    }

    // Releases all memory allocated for this Oak (should be used as part of the Oak destruction)
    // Not thread safe, should be a single thread call. (?)
    @Override
    public void close() {
        for (Block b : blocks) {
            pool.returnBlock(b);
        }
        pool.close();
        // no need to do anything with the free list,
        // as all free list members were residing on one of the (already released) blocks
    }

    // Returns the off-heap allocation of this OakMap
    @Override
    public long allocated() { return allocated.get(); }

    // used only for testing
    Block getCurrentBlock() {
        return currentBlock;
    }

    // used only for testing
    int numOfAllocatedBlocks() { return blocks.size(); }

    // used only for testing
    void stopMemoryReuse() { this.stopMemoryReuse = true; }
    void startMemoryReuse() { this.stopMemoryReuse = false; }
}