package com.oath.oak.NativeAllocator;

interface BlocksProvider {
    long blockSize();

    Block getBlock();

    void returnBlock(Block block);
}
