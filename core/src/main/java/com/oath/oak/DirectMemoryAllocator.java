package com.oath.oak;

import java.nio.ByteBuffer;

public class DirectMemoryAllocator implements OakMemoryAllocator{

    public DirectMemoryAllocator() {

    }

    public ByteBuffer allocate(int size) {
        return ByteBuffer.allocateDirect(size);
    }


    public void free(ByteBuffer bb) {

    }

    public void close() {

    }

    public long allocated() {
        return 0;
    }
}


