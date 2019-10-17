package com.oath.oak;

import java.nio.ByteBuffer;

import static com.oath.oak.NativeAllocator.OakNativeMemoryAllocator.INVALID_BLOCK_ID;

public class DirectMemoryAllocator implements OakMemoryAllocator {

    public DirectMemoryAllocator() {

    }

    public ByteBuffer allocate(int size) {
        return ByteBuffer.allocateDirect(size);
    }

    public Slice allocateSlice(int size) {
        return new Slice(INVALID_BLOCK_ID, allocate(size));
    }

    public void free(ByteBuffer bb) {

    }

    public void freeSlice(Slice slice) {

    }

    public void close() {

    }

    public long allocated() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}


