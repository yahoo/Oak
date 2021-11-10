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

    private static volatile BlocksPool instance = null;
    private final ConcurrentLinkedQueue<Block> blocks = new ConcurrentLinkedQueue<>();

    // TODO change the following constants to be configurable

    static final long MB = 1L << 20;
    static final long GB = 1L << 30;

    // The memory size to pre-allocate on initialization.
    private static final long PRE_ALLOC_SIZE_BYTES = 0;

    // The minimal memory size to be allocated at once when not enough memory is available.
    // If the used block size is larger than this number, exactly one block will be allocated.
    // Otherwise, more than one block might be allocated at once.
    private static final long NEW_ALLOC_MIN_SIZE_BYTES = 8L * MB;

    // Upper/lower thresholds to the quantity of the unused memory to reserve in the pool for future use.
    // When the unused memory quantity reaches HIGH_RESERVED_SIZE_BYTES, some memory is freed
    // such that the remaining unused memory will be LOW_RESERVED_SIZE_BYTES.
    private static final long LOW_RESERVED_SIZE_BYTES = 2L * GB;
    private static final long HIGH_RESERVED_SIZE_BYTES = 4L * GB;

    // The default size of a single memory block to be allocated at once.
    // This block size (currently) imposes off-heap memory limit of 128GB
    // for all Oak instances working via NativeAllocator with this pool
    static final int DEFAULT_BLOCK_SIZE_BYTES = 128 * (int) MB;

    /**
     * The block size in bytes that is used by this pool.
     * It is limited to an integer duo to similar limitation of {@code ByteBuffer::allocateDirect(int capacity)}.
     */
    private final int blockSizeBytes;

    private final int newAllocBlocks;
    private final int lowReservedBlocks;
    private final int highReservedBlocks;

    // not thread safe, private constructor; should be called only once
    private BlocksPool() {
        this(DEFAULT_BLOCK_SIZE_BYTES);
    }

    // Used internally and for tests.
    private BlocksPool(int blockSizeBytes) {
        this.blockSizeBytes = blockSizeBytes;
        this.newAllocBlocks = convertSizeToBlocks(NEW_ALLOC_MIN_SIZE_BYTES, 1);
        this.lowReservedBlocks = convertSizeToBlocks(LOW_RESERVED_SIZE_BYTES, 0);
        this.highReservedBlocks = convertSizeToBlocks(HIGH_RESERVED_SIZE_BYTES, this.lowReservedBlocks + 1);
        alloc(convertSizeToBlocks(PRE_ALLOC_SIZE_BYTES, 0));
    }

    private int convertSizeToBlocks(long sizeBytes, int minBlocks) {
        return Math.max(minBlocks, (int) Math.ceil((float) sizeBytes / (float) blockSizeBytes));
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

    /**
     * Sets the preferred block size. This only has an effect if the block pool was never used before.
     * @param preferredBlockSizeBytes the preferred block size
     * @return true if the preferred block size matches the current instance
     */
    static boolean preferBlockSize(int preferredBlockSizeBytes) {
        if (instance == null) {
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                if (instance == null) {
                    instance = new BlocksPool(preferredBlockSizeBytes);
                }
            }
        }

        return instance.blockSizeBytes == preferredBlockSizeBytes;
    }

    @Override
    public int blockSize() {
        return blockSizeBytes;
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
                        alloc(newAllocBlocks);
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
        if (blocks.size() > highReservedBlocks) { // too many unused blocks
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                while (blocks.size() > lowReservedBlocks) {
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

    private void alloc(int numOfBlocks) {
        // pre-allocation loop
        for (int i = 0; i < numOfBlocks; i++) {
            // The blocks are allocated without ids.
            // They are given an id when they are given to an OakNativeMemoryAllocator.
            this.blocks.add(new Block(blockSizeBytes));
        }
    }

    // used only for testing
    int numOfRemainingBlocks() {
        return blocks.size();
    }
}
