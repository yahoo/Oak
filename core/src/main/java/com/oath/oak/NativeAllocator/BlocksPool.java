/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak.NativeAllocator;

import java.util.concurrent.ConcurrentLinkedQueue;

/*
 * The singleton Pool to pre-allocate and reuse blocks of off-heap memory. The singleton has lazy
 * initialization so the big memory is allocated only on demand when first Oak is used.
 * However it makes creation of the first Oak slower. This initialization is thread safe, thus
 * multiple concurrent Oak creations will result only in the one Pool.
 * */
class BlocksPool implements BlocksProvider {

    private static BlocksPool instance = null;
    private final ConcurrentLinkedQueue<Block> blocks = new ConcurrentLinkedQueue<>();

    // TODO change BLOCK_SIZE and NUMBER_OF_BLOCKS to be pre-configurable
    private static int BLOCK_SIZE = 256 * 1024 * 1024; // currently 200MB, the one block size
    // Number of memory blocks to be pre-allocated (currently gives us 2GB). When it is not enough,
    // another half such amount of memory (1GB) will be allocated at once.
    private final static int NUMBER_OF_BLOCKS = 10;
    private final static int EXCESS_POOL_RATIO = 3;

    // not thread safe, private constructor; should be called only once
    private BlocksPool() {
        prealloc(NUMBER_OF_BLOCKS);
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

    static void setBlockSize(int blockSize) {
        BLOCK_SIZE = blockSize;
    }

    @Override
    public int blockSize() {
        return BLOCK_SIZE;
    }

    /**
     * Returns a single Block from within the Pool, enlarges the Pool if needed
     * Thread-safe
     */
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
                        prealloc(NUMBER_OF_BLOCKS / 2);
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
    public void returnBlock(Block b) {
        b.reset();
        blocks.add(b);
        if (blocks.size() > EXCESS_POOL_RATIO * NUMBER_OF_BLOCKS) { // too many unused blocks
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                for (int i = 0; i < NUMBER_OF_BLOCKS; i++) {
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
    void close() {
        while (!blocks.isEmpty()) {
            blocks.poll().clean();
        }
    }

    private void prealloc(int numOfBlocks) {
        // pre-allocation loop
        for (int i = 0; i < numOfBlocks; i++) {
            this.blocks.add(new Block(BlocksPool.BLOCK_SIZE));
        }
    }

    // used only for testing
    int numOfRemainingBlocks() {
        return blocks.size();
    }
}