/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak.NativeAllocator;

import com.oath.oak.OakOutOfMemoryException;
import com.oath.oak.Slice;
import com.oath.oak.ThreadIndexCalculator;
import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.NativeAllocator.OakNativeMemoryAllocator.INVALID_BLOCK_ID;
import static com.oath.oak.NativeAllocator.OakNativeMemoryAllocator.FIRST_THREAD_BUFFER;

class Block {

    private final ByteBuffer buffer;
    private final int capacity;
    private final AtomicInteger allocated = new AtomicInteger(0);
    private int id; // placeholder might need to be set in the future

    // in order to avoid creating a new ByteBuffer per each reading
    // (for example binary search through the keys)
    // keep persistent ByteBuffer objects referring to a slice from a Block's big underlying buffer
    // in order to make it thread-safe keep separate persistent ByteBuffer per thread
    private ThreadIndexCalculator threadIndexCalculator;
    private ByteBuffer[] byteBufferPerThread;

    // as a per thread ByteBuffer is used per thread, it can be impossible to do something
    // with reading two ByteBuffers concurrently, for example compare two keys located off-heap.
    // To enable this allow in rare cases to use second byteBufferPerThread
    private ByteBuffer[] secondByteBufferPerThread = null;

    Block(long capacity) {
        assert capacity > 0;
        assert capacity <= Integer.MAX_VALUE; // This is exactly 2GB
        this.capacity = (int) capacity;
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.byteBufferPerThread = new ByteBuffer[ThreadIndexCalculator.MAX_THREADS];
        this.id = INVALID_BLOCK_ID;
        // Pay attention in allocateDirect the data is *zero'd out*
        // which has an overhead in clearing and you end up touching every page
        this.buffer = ByteBuffer.allocateDirect(this.capacity);
    }

    void setID(int id) {
        this.id = id;
    }

    // Block manages its linear allocation. Thread safe.
    // The returned buffer doesn't have all zero bytes.
    Slice allocate(int size) {
        int now = allocated.getAndAdd(size);
        if (now + size > this.capacity) {
            allocated.getAndAdd(-size);
            throw new OakOutOfMemoryException();
        }
        // The position and limit of this thread's buffer are changed.
        // This means that a thread cannot allocate two slices from the same block at the same time without
        // duplicating one of them.
        ByteBuffer bb = getMyBuffer();
        bb.limit(now + size);
        bb.position(now);
        return new Slice(id, bb);
    }

    // use when this Block is no longer in any use, not thread safe
    // It sets the limit to the capacity and the position to zero, but didn't zeroes the memory
    void reset() {
        buffer.clear(); // reset the position
        allocated.set(0);
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
    }

    // return how many bytes are actually allocated for this block only, thread safe
    long allocated() {
        return allocated.get();
    }

    // releasing the memory back to the OS, freeing the block, an opposite of allocation, not thread safe
    void clean() {
        Field cleanerField = null;
        try {
            cleanerField = buffer.getClass().getDeclaredField("cleaner");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        assert cleanerField != null;
        cleanerField.setAccessible(true);
        Cleaner cleaner = null;
        try {
            cleaner = (Cleaner) cleanerField.get(buffer);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        assert cleaner != null;
        cleaner.clean();
    }

    private ByteBuffer getMyBuffer() {
        int idx = threadIndexCalculator.getIndex();
        if (byteBufferPerThread[idx] == null) {
            // the new buffer object is needed for thread safeness, otherwise
            // (in single threaded environment)
            // the setting of position and limit could happen on the main buffer itself
            // but it happens only once per thread id
            byteBufferPerThread[idx] = buffer.duplicate();
        }

        return byteBufferPerThread[idx];
    }

    private ByteBuffer getMySecondBuffer() {
        int idx = threadIndexCalculator.getIndex();
        if (secondByteBufferPerThread == null) {
            // lazy initialization
            this.secondByteBufferPerThread = new ByteBuffer[ThreadIndexCalculator.MAX_THREADS];
        }
        if (secondByteBufferPerThread[idx] == null) {
            // the new buffer object is needed for thread safeness, otherwise
            // (in single threaded environment)
            // the setting of position and limit could happen on the main buffer itself
            // but it happens only once per thread id
            secondByteBufferPerThread[idx] = buffer.duplicate();
        }

        return secondByteBufferPerThread[idx];
    }

    ByteBuffer getBufferForThread(int position, int length, int numerator) {
        ByteBuffer bb = (numerator == FIRST_THREAD_BUFFER) ? getMyBuffer() : getMySecondBuffer();
        bb.limit(position + length);
        bb.position(position);
        // on purpose not creating a ByteBuffer slice() here,
        // slice() will be used only per demand when buffer is passed outside Oak
        return bb;
    }

    // how many bytes a block may include, regardless allocated/free
    public int getCapacity() {
        return capacity;
    }

}