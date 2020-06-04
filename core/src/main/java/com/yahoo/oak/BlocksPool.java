/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.Closeable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The singleton Pool to pre-allocate and reuse blocks of off-heap memory. The singleton has lazy
 * initialization so the big memory is allocated only on demand when first Oak is used.
 * However it makes creation of the first Oak slower. This initialization is thread safe, thus
 * multiple concurrent Oak creations will result only in the one Pool.
 */
final class BlocksPool implements BlocksProvider, Closeable {

    private static BlocksPool instance = null;
    private final ConcurrentLinkedQueue<Block> blocks = new ConcurrentLinkedQueue<>();

    // TODO change the following constants to be configurable

    // Size of a single memory block - currently 256MB
    static final int BLOCK_SIZE = 256 * 1024 * 1024;

    // Number of memory blocks to be pre-allocated
    private static final int PRE_ALLOC_BLOCKS = 0;

    // Number of memory blocks to be allocated at once when not enough blocks available
    private static final int NEW_ALLOC_BLOCKS = 1;

    // Upper/lower thresholds to the number of unused memory blocks to reserve in the pool for future use.
    // When the number of unused blocks reaches HIGH_RESERVED_BLOCKS, a group of blocks will be freed
    // such that the number of blocks will be LOW_RESERVED_BLOCKS.
    private static final int HIGH_RESERVED_BLOCKS = 32;
    private static final int LOW_RESERVED_BLOCKS = 24;

    private final int blockSize;

    // not thread safe, private constructor; should be called only once
    private BlocksPool() {
        this.blockSize = BLOCK_SIZE;
        prealloc(PRE_ALLOC_BLOCKS);
    }

    // used in tests only!!
    private BlocksPool(int blockSize) {
        this.blockSize = blockSize;
        prealloc(PRE_ALLOC_BLOCKS);
    }

    /**
     * Initializes the instance of BlocksPool if not yet initialized, otherwise returns
     * the single instance of the singleton. Thread safe.
     */
    static BlocksPool getInstance() {
        if (instance == null) {
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                if (instance == null) {
                    instance = new BlocksPool();
                }
            }
        }
        return instance;
    }

    // used only in OakNativeMemoryAllocatorTest.java
    static void setBlockSize(int blockSize) {
        synchronized (BlocksPool.class) { // can be easily changed to lock-free
            if (instance != null) {
                instance.close();
            }
            instance = new BlocksPool(blockSize);
        }
    }

    @Override
    public int blockSize() {
        return blockSize;
    }

    /**
     * Returns a single Block from within the Pool, enlarges the Pool if needed
     * Thread-safe
     */
    @Override
    public Block getBlock() {
        Block b = null;
        while (b == null) {
            boolean noMoreBlocks = blocks.isEmpty();
            if (!noMoreBlocks) {
                b = blocks.poll();
            }

            if (noMoreBlocks || b == null) {
                synchronized (BlocksPool.class) { // can be easily changed to lock-free
                    if (blocks.isEmpty()) {
                        prealloc(NEW_ALLOC_BLOCKS);
                    }
                }
            }
        }
        return b;
    }

    /**
     * Returns a single Block to the Pool, decreases the Pool if needed
     * Assumes block is not used by any concurrent thread, otherwise thread-safe
     */
    @Override
    public void returnBlock(Block b) {
        b.reset();
        blocks.add(b);
        if (blocks.size() > HIGH_RESERVED_BLOCKS) { // too many unused blocks
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                while (blocks.size() > LOW_RESERVED_BLOCKS) {
                    this.blocks.poll().clean();
                }
            }
        }
    }

    /**
     * Should be called when the entire Pool is not used anymore. Releases the memory only of the
     * blocks returned back to the pool.
     * However this object is GCed when the entire process dies, but thus all the memory is released
     * anyway...
     */
    @Override
    public void close() {
        while (!blocks.isEmpty()) {
            blocks.poll().clean();
        }
    }

    private void prealloc(int numOfBlocks) {
        // pre-allocation loop
        for (int i = 0; i < numOfBlocks; i++) {
            // The blocks are allocated without ids.
            // They are given an id when they are given to an OakNativeMemoryAllocator.
            this.blocks.add(new Block(blockSize));
        }
    }

    // used only for testing
    int numOfRemainingBlocks() {
        return blocks.size();
    }
}
