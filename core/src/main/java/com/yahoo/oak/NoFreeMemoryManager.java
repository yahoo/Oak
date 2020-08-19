/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


class NoFreeMemoryManager implements MemoryManager {
    private final BlockMemoryAllocator allocator;

    /*
     * The KEY_RC reference codec encodes the reference of the keys into a single long primitive (64 bit).
     * For the default block size (256MB), we need 28 bits to encode the offset
     * and additional 28 bits to encode the length.
     * So, the remaining 8 bits can encode the block id, which will limit the maximal number of blocks to 256.
     * Thus, the key/value reference encoding when using the default block size (256MB) will be as follows:
     *
     *    LSB                                       MSB
     *     |     offset     |     length     | block |
     *     |     28 bit     |     28 bit     | 8 bit |
     *      0             27 28            55 56   63
     *
     * From that, we can derive that the maximal number of 1K items that can be allocated is ~128 million (2^26).
     * Note: these limitations will change for different block sizes.
     *
     */
    private final ReferenceCodecDirect rcd;

    NoFreeMemoryManager(BlockMemoryAllocator memoryAllocator) {
        assert memoryAllocator != null;
        this.allocator = memoryAllocator;
        rcd = new ReferenceCodecDirect(
            BlocksPool.getInstance().blockSize(), BlocksPool.getInstance().blockSize(), this);
    }

    public void close() {
        allocator.close();
    }

    public long allocated() {
        return allocator.allocated();
    }

    @Override
    public void allocate(Slice s, int size, Allocate allocate) {
        boolean allocated = allocator.allocate(s, size, allocate);
        assert allocated;
    }

    @Override
    public void release(Slice s) {
    }

    @Override
    public void readByteBuffer(Slice s, int blockID) {
        allocator.readByteBuffer(s, blockID);
    }

    public boolean isClosed() {
        return allocator.isClosed();
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    /**
     * Get ReferenceCodec to manage the (long) references,
     * in which all the info for the memory access is incorporated
     *
     */
    @Override public ReferenceCodec getReferenceCodec() {
        return rcd;
    }
}

