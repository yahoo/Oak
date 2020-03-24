/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

/*
 * The OakRReference allows reuse of the same OakRBuffer implementation object and is used for
 * Oak's StreamIterators, where the iterated OakRBuffers can be used only once.
 * This class is actually a reference into internal BB object rather than new BB object.
 * It references the internal BB object as far as OakRReference wasn't moved to point on other BB.
 *
 * The OakRReference is intended to be used in threads that are for iterations only and are not involved in
 * concurrent/parallel reading/updating the mappings
 *
 * Unlike other ephemeral objects, even if OakRReference references a value it does not have to acquire a read lock
 * before each access since it can only be used without other concurrent writes in the background.
 * */

public class OakRKeyBufferImpl implements OakRBuffer, OakUnsafeDirectBuffer {

    private int blockID = OakNativeMemoryAllocator.INVALID_BLOCK_ID;
    private int position = -1;
    private int length = -1;
    private final MemoryManager memoryManager;
    private final int headerSize;

    // The OakRReference user accesses OakRReference as it would be a ByteBuffer with initially zero position.
    // We translate it to the relevant ByteBuffer position, by adding keyPosition and the header size to any given index

    OakRKeyBufferImpl(MemoryManager memoryManager, int headerSize) {
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
        ByteBuffer buffer = getTemporaryPerThreadByteBuffer().asReadOnlyBuffer();
        if (headerSize != 0) {
            buffer.position(buffer.position() + headerSize);
        }
        buffer = buffer.slice();
        return transformer.apply(buffer);
    }

    private ByteBuffer getTemporaryPerThreadByteBuffer() {
        // No access is allowed once the memory manager is closed.
        // We avoid validating this here due to performance concerns.
        // The correctness is persevered because when the memory manager is closed,
        // its block array is no longer reachable.
        // Thus, a null pointer exception will be raised once we try to get the byte buffer.
        assert blockID != OakNativeMemoryAllocator.INVALID_BLOCK_ID;
        assert position != -1;
        assert length != -1;
        return memoryManager.getByteBufferFromBlockID(blockID, position, length);
    }

    /*-------------- OakUnsafeRef --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer buff = getTemporaryPerThreadByteBuffer().asReadOnlyBuffer();
        buff.position(headerSize + position);
        buff.limit(headerSize + position + length);
        return buff.slice();
    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public long getAddress() {
        ByteBuffer buff = getTemporaryPerThreadByteBuffer();
        long address = ((DirectBuffer) buff).address();
        return address + headerSize + position;
    }
}
