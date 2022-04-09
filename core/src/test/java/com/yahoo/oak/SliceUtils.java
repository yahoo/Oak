/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


class SliceUtils implements AutoCloseable {

    private final MemoryManager memoryManager;

    SliceUtils() {
        int size = 8 * 1024 * 1024;
        long capacity = size * 3;
        BlocksPool.setBlockSize(size);
        memoryManager = new SyncRecycleMemoryManager(new NativeMemoryAllocator(capacity));
    }

    Slice getEmptySlice() {
        return memoryManager.getEmptySlice();
    }

    @Override
    public void close() throws Exception {
        memoryManager.close();
        BlocksPool.setBlockSize(BlocksPool.DEFAULT_BLOCK_SIZE_BYTES);
    }
}
