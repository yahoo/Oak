/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.concurrent.ConcurrentLinkedQueue;

/*
* The singleton Pool to pre-allocate and reuse blocks of off-heap memory. The singleton has lazy
* initialization so the big memory is allocated only on demand when first Oak is used.
* However it makes creation of the first Oak slower. This initialization is thread safe, thus
* multiple concurrent Oak creations will result only in the one Pool.
* */
class BlocksPool {

    private static BlocksPool instance = null;
    private ConcurrentLinkedQueue<Block> blocks = new ConcurrentLinkedQueue<Block>();

    // TODO change BLOCK_SIZE and NUMBER_OF_BLOCKS to be pre-configurable, currently changeable for tests
    public final static int BLOCK_SIZE = 104857600; // currently 100MB, the one block size
    // Number of memory blocks to be pre-allocated (currently gives us 2GB). When it is not enough,
    // another such amount of memory will be allocated at once.
    public final static int NUMBER_OF_BLOCKS = 20;

    // not thread safe, private constructor; should be called only once
    private BlocksPool(int blockSize, int numOfBlocks) {
        assert blockSize > 0;
        assert numOfBlocks <= Integer.MAX_VALUE;
        prealloc(BLOCK_SIZE, NUMBER_OF_BLOCKS);
    }

    /**
     * Initializes the instance of BlocksPool if not yet initialized, otherwise returns
     * the single instance of the singleton. Thread safe.
     */
    static BlocksPool getInstance() {
        if (instance == null) {
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                if (instance == null) {
                    instance = new BlocksPool(BLOCK_SIZE, NUMBER_OF_BLOCKS);
                }
            }
        }
        return instance;
    }

    /**
     * Returns a single Block from within the Pool, enlarges the Pool if needed
     * Thread-safe
     */
    Block getBlock() {
        Block b = null;
        while (b == null) {
            boolean noMoreBlocks = blocks.isEmpty();
            if (!noMoreBlocks) {
                b = blocks.poll();
            }

            if (noMoreBlocks || b == null) {
                synchronized (BlocksPool.class) { // can be easily changed to lock-free
                    if (blocks.isEmpty()) {
                        prealloc(BLOCK_SIZE, NUMBER_OF_BLOCKS/2);
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
    void returnBlock(Block b) {
        b.reset();
        blocks.add(b);
        if (blocks.size() > 3*NUMBER_OF_BLOCKS) {
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                for (int i=0; i<NUMBER_OF_BLOCKS; i++) {
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

    private void prealloc(int blockSize, int numOfBlocks) {
        // pre-allocation loop
        for (int i=0; i<numOfBlocks; i++) {
            this.blocks.add(new Block(blockSize));
        }
    }

    // used only for testing
    int numOfRemainingBlocks() {
        return blocks.size();
    }
}