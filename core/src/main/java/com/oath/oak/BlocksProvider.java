package com.oath.oak;

interface BlocksProvider {
    int blockSize();

    Block getBlock();

    void returnBlock(Block block);
}
