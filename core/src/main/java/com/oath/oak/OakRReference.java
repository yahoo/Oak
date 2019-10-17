/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ConcurrentModificationException;
import java.util.function.Function;

/*
 * The OakRKeyReference allows reuse of the same OakRBuffer implementation object and is used for
 * Oak's StreamIterators, where the iterated OakRBuffers can be used only once.
 * This class is actually a reference into internal BB object rather than new BB object.
 * It references the internal BB object as far as OakRKeyReference wasn't moved to point on other BB.
 *
 * The OakRKeyReference is intended to be used in threads that are for iterations only
 * and are not involved in concurrent/parallel reading/updating the mappings
 * */

// TODO: No method acquires a read lock
public class OakRReference implements OakRBuffer {

    private int blockID = OakNativeMemoryAllocator.INVALID_BLOCK_ID;
    private int position = -1;
    private int length = -1;
    private final NovaManager memoryManager;
    private final int headerSize;

    // The OakRKeyReferBufferImpl user accesses OakRKeyReferBufferImpl
    // as it would be a ByteBuffer with initially zero position.
    // We translate it to the relevant ByteBuffer position, by adding keyPosition to any given index

    OakRReference(NovaManager memoryManager, int headerSize) {
        this.memoryManager = memoryManager;
        this.headerSize = headerSize;
    }

    void setReference(int blockID, int position, int length) {
        this.blockID = blockID;
        this.position = position;
        this.length = length;
    }

    void setPosition(int position) {
        this.position = position;
    }

    void setLength(int length) {
        this.length = length;
    }

    @Override
    public int capacity() {
        return getTemporaryPerThreadByteBuffer().capacity();
    }

    @Override
    public byte get(int index) {
        return getTemporaryPerThreadByteBuffer().get(index + headerSize + position);
    }

    @Override
    public ByteOrder order() {
        return getTemporaryPerThreadByteBuffer().order();
    }

    @Override
    public char getChar(int index) {
        return getTemporaryPerThreadByteBuffer().getChar(index + headerSize + position);
    }

    @Override
    public short getShort(int index) {
        return getTemporaryPerThreadByteBuffer().getShort(index + headerSize + position);
    }

    @Override
    public int getInt(int index) {
        return getTemporaryPerThreadByteBuffer().getInt(index + headerSize + position);
    }

    @Override
    public long getLong(int index) {
        return getTemporaryPerThreadByteBuffer().getLong(index + headerSize + position);
    }

    @Override
    public float getFloat(int index) {
        return getTemporaryPerThreadByteBuffer().getFloat(index + headerSize + position);
    }

    @Override
    public double getDouble(int index) {
        return getTemporaryPerThreadByteBuffer().getChar(index + headerSize + position);
    }

    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        // The new ByteBuffer object is created here via slice(), to be sure that (user provided)
        // transformer can not access anything beyond given ByteBuffer
        ByteBuffer buffer = getTemporaryPerThreadByteBuffer().slice();
        if (headerSize != 0) {
            buffer.position(headerSize);
            buffer = buffer.slice();
        }
        return transformer.apply(buffer);
    }

    // the method provides an optimal way to copy data as array of integers from the key's buffer
    // srcPosition - index from where to start copying the internal key buffer,
    // srcPosition = 0 if to start from the beginning
    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getTemporaryPerThreadByteBuffer().slice(),
                srcPosition + position + headerSize, dstArray, countInts);

    }

    private ByteBuffer getTemporaryPerThreadByteBuffer() {
        // need to check that the entire OakMap wasn't already closed with its memoryManager
        // if memoryManager was closed, its object is still not GCed as it is pointed from here
        // therefore it is valid to check from here
        if (memoryManager.isClosed()) {
            throw new ConcurrentModificationException();
        }
        assert blockID != OakNativeMemoryAllocator.INVALID_BLOCK_ID;
        assert position != -1;
        assert length != -1;
        return memoryManager.getByteBufferFromBlockID(blockID, position, length);
    }
}
