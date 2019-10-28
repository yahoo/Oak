package com.oath.oak;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface MemoryManager extends Closeable {

    enum Allocate {
        KEY, VALUE;
    }

    boolean isClosed();

    long allocated();

    Slice allocateSlice(int size, Allocate allocate);

    void releaseSlice(Slice s);

    Slice getSliceFromBlockID(int BlockID, int bufferPosition, int bufferLength);

    ByteBuffer getByteBufferFromBlockID(int BlockID, int bufferPosition, int bufferLength);
}
