/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * An instance of this buffer is only used when the write lock of the key/value referenced by it is already acquired.
 * This is the reason no lock is acquired in each access.
 */
class OakAttachedWriteBuffer extends OakAttachedReadBuffer implements OakWriteBuffer, OakUnsafeDirectBuffer {

    private boolean enabled = true;

    /**
     * This class is instantiated only internally to ensure that the buffer is disabled for writes once the scope
     * is finished.
     * @param s the buffer to use.
     */
    private OakAttachedWriteBuffer(Slice s) {
        super(s);
    }

    /**
     * Serialize an object using this class, following three steps:
     * (1) instantiate a new OakAttachedWriteBuffer object from the input Slice
     * (2) serialize the input object to this buffer
     * (3) disable the OakAttachedWriteBuffer
     * This procedure ensures no out of scope writes will be possible
     * @param s          the buffer to write to
     * @param obj        the object to write
     * @param serializer the serialization method
     */
    static <T> void serialize(Slice s, T obj, OakSerializer<T> serializer) {
        OakAttachedWriteBuffer writeBuffer = new OakAttachedWriteBuffer(s);
        serializer.serialize(obj, writeBuffer);
        writeBuffer.enabled = false;
    }

    /**
     * Perform an update on an object using this class, following three steps:
     * (1) instantiate a new OakAttachedWriteBuffer object from the input Slice
     * (2) perform the update on this buffer
     * (3) disable the OakAttachedWriteBuffer
     * This procedure ensures no out of scope writes will be possible
     * @param s        the buffer to write to
     * @param computer the update method
     */
    static void compute(Slice s, Consumer<OakWriteBuffer> computer) {
        OakAttachedWriteBuffer writeBuffer = new OakAttachedWriteBuffer(s);
        computer.accept(writeBuffer);
        writeBuffer.enabled = false;
    }

    void validateAccess() {
        if (!enabled) {
            throw new RuntimeException("Attached buffer cannot be used outside of its attached scope.");
        }
    }

    @Override
    public OakWriteBuffer put(int index, byte value) {
        validateAccess();
        writeBuffer.put(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putChar(int index, char value) {
        validateAccess();
        writeBuffer.putChar(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putShort(int index, short value) {
        validateAccess();
        writeBuffer.putShort(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putInt(int index, int value) {
        validateAccess();
        writeBuffer.putInt(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putLong(int index, long value) {
        validateAccess();
        writeBuffer.putLong(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putFloat(int index, float value) {
        validateAccess();
        writeBuffer.putFloat(getDataOffset(index), value);
        return this;
    }

    @Override
    public OakWriteBuffer putDouble(int index, double value) {
        validateAccess();
        writeBuffer.putDouble(getDataOffset(index), value);
        return this;
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        validateAccess();
        return writeBuffer;
    }
}
