package com.oath.oak;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface MemoryManager extends Closeable {

    /**
     * This enum indicates whether the slice allocated will belong to a key or to a value.
     */
    enum Allocate {
        KEY, VALUE;
    }

    boolean isClosed();

    /**
     * @return the number of bytes allocated by the {@code allocateSlice} method.
     */
    long allocated();

    /**
     * This method allocates a Slice out of the off-heap, i.e., the ByteBuffer inside of the Slice is pointing to the
     * off-heap. The blockID, is an internal reference to which block the ByteBuffer points, allowing the functions
     * {@code getSliceFromBlockID} and {@code getByteBufferFromBlockID} to reconstruct the same ByteBuffer.
     *
     * @param size     - the size of the Slice to allocate
     * @param allocate - whether this Slice is for a key or a value
     * @return the newly allocated Slice
     */
    Slice allocateSlice(int size, Allocate allocate);

    /**
     * When returning a Slice to the Memory Manager, depending on the implementation, there might be a restriction on
     * whether this Slice is reachable by other threads or not.
     *
     * @param s the slice to release
     */
    void releaseSlice(Slice s);

    default Slice getSliceFromBlockID(int blockID, int bufferPosition, int bufferLength) {
        return new Slice(
            blockID,
            getByteBufferFromBlockID(blockID, bufferPosition, bufferLength));
    }

    /**
     * Translates the trio of blockID, position and length (a.k.a reference) into a ByteBuffer.
     *
     * @return the reconstructed ByteBuffer
     */
    ByteBuffer getByteBufferFromBlockID(int blockID, int bufferPosition, int bufferLength);

    /**
     * TODO Liran: This should be documented. Why is the version handled in the memory allocator?
     *
     * @return the current version
     */
    int getCurrentVersion();
}
