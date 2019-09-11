/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class OakRKeyReferenceImpl implements OakRBuffer {

    private int blockID = 0;
    private int keyPosition = 0;
    private int keyLength = 0;
    private final MemoryManager memoryManager;

    private ByteBuffer bb = null;

    // The OakRKeyReferBufferImpl user accesses OakRKeyReferBufferImpl
    // as it would be a ByteBuffer with initially zero position.
    // We translate it to the relevant ByteBuffer position, by adding keyPosition to any given index

    OakRKeyReferenceImpl(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    public void setBlockID(int blockID) {
        this.blockID = blockID;
        this.bb = null;
    }

    public void setKeyPosition(int keyPosition) {
        this.keyPosition = keyPosition;
        this.bb = null;
    }

    public void setKeyLength(int keyLength) {
        this.keyLength = keyLength;
        this.bb = null;
    }

    @Override
    public int capacity() {
        return getTemporalPerThreadByteBuffer().capacity();
    }

    @Override
    public byte get(int index) {
        return getTemporalPerThreadByteBuffer().get(index+keyPosition);
    }

    @Override
    public ByteOrder order() {
        return getTemporalPerThreadByteBuffer().order();
    }

    @Override
    public char getChar(int index) {
        return getTemporalPerThreadByteBuffer().getChar(index+keyPosition);
    }

    @Override
    public short getShort(int index) {
        return getTemporalPerThreadByteBuffer().getShort(index+keyPosition);
    }

    @Override
    public int getInt(int index) {
        return getTemporalPerThreadByteBuffer().getInt(index+keyPosition);
    }

    @Override
    public long getLong(int index) {
        return getTemporalPerThreadByteBuffer().getLong(index+keyPosition);
    }

    @Override
    public float getFloat(int index) {
        return getTemporalPerThreadByteBuffer().getFloat(index+keyPosition);
    }

    @Override
    public double getDouble(int index) {
        return getTemporalPerThreadByteBuffer().getChar(index+keyPosition);
    }

    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        // TODO: here we might need slice() or to be sure transformer doesn't assume starting at position 0
        return transformer.apply(getTemporalPerThreadByteBuffer());
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getTemporalPerThreadByteBuffer(), srcPosition+keyPosition, dstArray, countInts);

    }

    private ByteBuffer getTemporalPerThreadByteBuffer() {
        // TODO: how to check that the entire Oak wasn't already closed with its memoryManager

        if (bb != null) return  bb;
        ByteBuffer bb = memoryManager.getByteBufferFromBlockID(blockID, keyPosition,keyLength);
        return bb;
    }
}
