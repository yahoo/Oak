/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/*
* The singleton Pool to pre-allocate and reuse blocks of off-heap memory. The singleton has lazy
* initialization so the big memory is allocated only on demand when first Oak is used.
* However it makes creation of the first Oak slower. This initialization is thread safe, thus
* multiple concurrent Oak creations will result only in the one Pool.
* */
class BlocksPool {

//    private static BlocksPool instance = null;
    private final ConcurrentLinkedQueue<Block> blocks = new ConcurrentLinkedQueue<Block>();

    // TODO change BLOCK_SIZE and NUMBER_OF_BLOCKS to be pre-configurable, currently changeable for tests
    public final static int BLOCK_SIZE = 104857600; // currently 100MB, the one block size
    // Number of memory blocks to be pre-allocated (currently gives us 2GB). When it is not enough,
    // another such amount of memory will be allocated at once.
    public final static int NUMBER_OF_BLOCKS = 20;

    private final AtomicInteger users;

    // not thread safe, private constructor; should be called only once
    public BlocksPool() {
        users = new AtomicInteger(0);
        prealloc(BLOCK_SIZE, NUMBER_OF_BLOCKS);
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
     * Should be called on each allocator close. Only last allocator will free all the blocks
     */
    void close() {
        if (users.decrementAndGet() > 0)
            return;
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

    public void registerAllocator() {
        users.incrementAndGet();
    }

}