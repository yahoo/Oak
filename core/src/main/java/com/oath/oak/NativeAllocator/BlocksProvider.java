package com.oath.oak.NativeAllocator;

interface BlocksProvider {
    int blockSize();

    Block getBlock();

    void returnBlock(Block block);
}
