/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class Block {

    private final ByteBuffer buffer;

    private final int capacity;
    private final AtomicInteger allocated = new AtomicInteger(0);
    private int id; // placeholder might need to be set in the future

    Block(long capacity) {
        assert capacity > 0;
        assert capacity <= Integer.MAX_VALUE; // This is exactly 2GB
        this.capacity = (int) capacity;
        this.id = NativeMemoryAllocator.INVALID_BLOCK_ID;
        // Pay attention in allocateDirect the data is *zero'd out*
        // which has an overhead in clearing and you end up touching every page
        this.buffer = ByteBuffer.allocateDirect(this.capacity);
    }

    void setID(int id) {
        this.id = id;
    }

    // Block manages its linear allocation. Thread safe.
    // The returned buffer doesn't have all zero bytes.
    boolean allocate(Slice s, int size) {
        int now = allocated.getAndAdd(size);
        if (now + size > this.capacity) {
            allocated.getAndAdd(-size);
            throw new OakOutOfMemoryException();
        }
        s.update(id, now, size);
        readByteBuffer(s);
        return true;
    }

    // use when this Block is no longer in any use, not thread safe
    // It sets the limit to the capacity and the position to zero, but didn't zeroes the memory
    void reset() {
        allocated.set(0);
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

    void readByteBuffer(Slice s) {
        s.setBuffer(buffer);
    }

    // how many bytes a block may include, regardless allocated/free
    public int getCapacity() {
        return capacity;
    }

}
