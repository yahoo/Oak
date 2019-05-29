package com.oath.oak.NativeAllocator;

import com.oath.oak.OakMemoryAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.nio.ByteBuffer;

public class NettyMemoryAllocator{
    private final ByteBufAllocator allocator;

    public NettyMemoryAllocator() {
        allocator = PooledByteBufAllocator.DEFAULT;
    }


    public ByteBuf allocate(int size) {
        return allocator.directBuffer(size);
    }

    public long allocated() {
        return 0;
    }
}
