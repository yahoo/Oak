package com.oath.oak;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface MemoryManager extends Closeable {

    boolean isClosed();

    long allocated();

    Slice allocateSlice(int size, boolean isKey);

    void releaseSlice(Slice s);

    Slice getSliceFromBlockID(int BlockID, int bufferPosition, int bufferLength);

    ByteBuffer getByteBufferFromBlockID(int BlockID, int bufferPosition, int bufferLength);
}
