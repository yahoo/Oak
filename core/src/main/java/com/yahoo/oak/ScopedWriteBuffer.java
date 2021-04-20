/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.function.Consumer;

/**
 * An instance of this buffer is only used when the write lock of the key/value referenced by it is already acquired.
 * This is the reason no lock is acquired in each access.
 */
final class ScopedWriteBuffer extends ScopedReadBuffer implements OakScopedWriteBuffer, OakUnsafeDirectBuffer {

    private boolean enabled = true;

    /**
     * This class is instantiated only internally to ensure that the buffer is disabled for writes once the scope
     * is finished.
     *
     * @param s the buffer to use.
     */
    private ScopedWriteBuffer(Slice s) {
        super(s.getDuplicatedSlice());
    }

    /**
     * Serialize an object using this class, following three steps:
     * (1) instantiate a new ScopedWriteBuffer object from the input Slice
     * (2) serialize the input object to this buffer
     * (3) disable the ScopedWriteBuffer
     * This procedure ensures no out of scope writes will be possible
     *
     * @param s          the buffer to write to
     * @param obj        the object to write
     * @param serializer the serialization method
     */
    static <T> void serialize(Slice s, T obj, OakSerializer<T> serializer) {
        ScopedWriteBuffer writeBuffer = new ScopedWriteBuffer(s);
        serializer.serialize(obj, writeBuffer);
        writeBuffer.enabled = false;
    }

    /**
     * Perform an update on an object using this class, following three steps:
     * (1) instantiate a new ScopedWriteBuffer object from the input Slice
     * (2) perform the update on this buffer
     * (3) disable the ScopedWriteBuffer
     * This procedure ensures no out of scope writes will be possible
     *
     * @param s        the buffer to write to
     * @param computer the update method
     */
    static void compute(Slice s, Consumer<OakScopedWriteBuffer> computer) {
        ScopedWriteBuffer writeBuffer = new ScopedWriteBuffer(s);
        computer.accept(writeBuffer);
        writeBuffer.enabled = false;
    }

    void validateAccess() {
        if (!enabled) {
            throw new RuntimeException("Scoped buffer cannot be used outside of its attached scope.");
        }
    }

    @Override
    public OakScopedWriteBuffer put(int index, byte value) {
        validateAccess();
        UnsafeUtils.put(getDataAddress(index), value);
        return this;
    }

    @Override
    public OakScopedWriteBuffer putChar(int index, char value) {
        validateAccess();
        UnsafeUtils.putChar(getDataAddress(index), value);
        return this;
    }

    @Override
    public OakScopedWriteBuffer putShort(int index, short value) {
        validateAccess();
        UnsafeUtils.putShort(getDataAddress(index), value);
        return this;
    }

    @Override
    public OakScopedWriteBuffer putInt(int index, int value) {
        validateAccess();
        UnsafeUtils.putInt(getDataAddress(index), value);
        return this;
    }

    @Override
    public OakScopedWriteBuffer putLong(int index, long value) {
        validateAccess();
        UnsafeUtils.putLong(getDataAddress(index), value);
        return this;
    }

    @Override
    public OakScopedWriteBuffer putFloat(int index, float value) {
        validateAccess();
        UnsafeUtils.putFloat(getDataAddress(index), value);
        return this;
    }

    @Override
    public OakScopedWriteBuffer putDouble(int index, double value) {
        validateAccess();
        UnsafeUtils.UNSAFE.putDouble(getDataAddress(index), value);
        return this;
    }
}
