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
public class OakRKeyReference implements OakRBuffer {

    private int blockID = OakNativeMemoryAllocator.INVALID_BLOCK_ID;
    private int keyPosition = -1;
    private int keyLength = -1;
    private final MemoryManager memoryManager;

    private ByteBuffer cachedBBreference = null;

    // The OakRKeyReferBufferImpl user accesses OakRKeyReferBufferImpl
    // as it would be a ByteBuffer with initially zero position.
    // We translate it to the relevant ByteBuffer position, by adding keyPosition to any given index

    OakRKeyReference(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    void setKeyReference(int blockID, int keyPosition, int keyLength) {
        this.blockID = blockID;
        this.keyPosition = keyPosition;
        this.keyLength = keyLength;
        this.cachedBBreference = null;
    }

    void setKeyPosition(int keyPosition) {
        this.keyPosition = keyPosition;
        this.cachedBBreference = null;
    }

    void setKeyLength(int keyLength) {
        this.keyLength = keyLength;
        this.cachedBBreference = null;
    }

    @Override
    public int capacity() {
        return getTemporaryPerThreadByteBuffer().capacity();
    }

    @Override
    public byte get(int index) {
        return getTemporaryPerThreadByteBuffer().get(index + keyPosition);
    }

    @Override
    public ByteOrder order() {
        return getTemporaryPerThreadByteBuffer().order();
    }

    @Override
    public char getChar(int index) {
        return getTemporaryPerThreadByteBuffer().getChar(index + keyPosition);
    }

    @Override
    public short getShort(int index) {
        return getTemporaryPerThreadByteBuffer().getShort(index + keyPosition);
    }

    @Override
    public int getInt(int index) {
        return getTemporaryPerThreadByteBuffer().getInt(index + keyPosition);
    }

    @Override
    public long getLong(int index) {
        return getTemporaryPerThreadByteBuffer().getLong(index + keyPosition);
    }

    @Override
    public float getFloat(int index) {
        return getTemporaryPerThreadByteBuffer().getFloat(index + keyPosition);
    }

    @Override
    public double getDouble(int index) {
        return getTemporaryPerThreadByteBuffer().getChar(index + keyPosition);
    }

    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        // The ne ByteBuffer object is created here via slice(), to be sure that (user provided)
        // transformer can not access anything beyond given ByteBuffer
        return transformer.apply(getTemporaryPerThreadByteBuffer().slice());
    }

    // the method provides an optimal way to copy data as array of integers from the key's buffer
    // srcPosition - index from where to start copying the internal key buffer,
    // srcPosition = 0 if to start from the beginning
    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getTemporaryPerThreadByteBuffer(), srcPosition + keyPosition, dstArray
                , countInts);

    }

    private ByteBuffer getTemporaryPerThreadByteBuffer() {
        if (cachedBBreference != null) {
            return cachedBBreference;
        }
        // need to check that the entire OakMap wasn't already closed with its memoryManager
        // if memoryManager was closed, its object is still not GCed as it is pointed from here
        // therefore it is valid to check from here
        if (memoryManager.isClosed()) {
            throw new ConcurrentModificationException();
        }
        assert blockID != OakNativeMemoryAllocator.INVALID_BLOCK_ID;
        assert keyPosition != -1;
        assert keyLength != -1;
        return memoryManager.getByteBufferFromBlockID(blockID, keyPosition, keyLength);
    }
}
