/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

import static com.oath.oak.Chunk.BLOCK_ID_LENGTH_ARRAY_INDEX;
import static com.oath.oak.Chunk.KEY_BLOCK_SHIFT;
import static com.oath.oak.Chunk.KEY_LENGTH_MASK;
import static com.oath.oak.Chunk.POSITION_ARRAY_INDEX;

public class OakRKeyBufferImpl implements OakRBuffer {

    private final long keyReference;
    private final MemoryManager memoryManager;
    private final int initialPosition;

    OakRKeyBufferImpl(long keyReference, MemoryManager memoryManager) {
        this.keyReference = keyReference;
        this.memoryManager = memoryManager;
        this.initialPosition = UnsafeUtils.longToInts(keyReference)[POSITION_ARRAY_INDEX];
    }

    private ByteBuffer getKeyBuffer() {
        int[] keyArray = UnsafeUtils.longToInts(keyReference);
        return memoryManager.getByteBufferFromBlockID(keyArray[BLOCK_ID_LENGTH_ARRAY_INDEX] >>> KEY_BLOCK_SHIFT, keyArray[POSITION_ARRAY_INDEX],
                keyArray[BLOCK_ID_LENGTH_ARRAY_INDEX] & KEY_LENGTH_MASK);
    }

    @Override
    public int capacity() {
        return UnsafeUtils.longToInts(keyReference)[BLOCK_ID_LENGTH_ARRAY_INDEX] & KEY_LENGTH_MASK;
    }

    @Override
    public byte get(int index) {
        return getKeyBuffer().get(index + initialPosition);
    }

    @Override
    public ByteOrder order() {
        return getKeyBuffer().order();
    }

    @Override
    public char getChar(int index) {
        return getKeyBuffer().getChar(index + initialPosition);
    }

    @Override
    public short getShort(int index) {
        return getKeyBuffer().getShort(index + initialPosition);
    }

    @Override
    public int getInt(int index) {
        return getKeyBuffer().getInt(index + initialPosition);
    }

    @Override
    public long getLong(int index) {
        return getKeyBuffer().getLong(index + initialPosition);
    }

    @Override
    public float getFloat(int index) {
        return getKeyBuffer().getFloat(index + initialPosition);
    }

    @Override
    public double getDouble(int index) {
        return getKeyBuffer().getChar(index + initialPosition);
    }

    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        return transformer.apply(getKeyBuffer().slice().asReadOnlyBuffer());
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getKeyBuffer().slice(), srcPosition, dstArray, countInts);
    }

}
