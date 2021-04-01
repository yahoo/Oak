/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicLong;

class Block {

    private final long memAddress;

    private final int capacity;
    private final AtomicLong allocated = new AtomicLong(0);
    private int id; // placeholder might need to be set in the future

    Block(long capacity) {
        assert capacity > 0;
        assert capacity <= Integer.MAX_VALUE; // This is exactly 2GiB
        this.capacity = (int) capacity;
        this.id = NativeMemoryAllocator.INVALID_BLOCK_ID;
        // Pay attention in allocateDirect the data is *zero'd out*
        // which has an overhead in clearing and you end up touching every page
        this.memAddress = UnsafeUtils.unsafe.allocateMemory(this.capacity);

    }

    void setID(int id) {
        this.id = id;
    }

    // Block manages its linear allocation. Thread safe.
    // The returned buffer doesn't have all zero bytes.
    boolean allocate(Slice s, final int size) {
        assert size > 0;
        long now = allocated.get();
        if (now + size <= this.capacity) { // check is only an optimization
            now = allocated.getAndAdd(size);
        }
        if (now + size > this.capacity) {
            throw new OakOutOfMemoryException(String.format("Block %d is out of memory", id));
        }
        s.associateBlockAllocation(id, (int) now, size, memAddress, capacity);
        return true;
    }

    // use when this Block is no longer in any use, not thread safe
    // It sets the limit to the capacity and the position to zero, but didn't zeroes the memory
    void reset() {
        allocated.set(0);
    }

    // return upperbound of bytes actually allocated for this block only, thread safe
    // the returned value can be greater than the actual bytes allocated
    // do not use this for exact comparison or any precise computation
    long allocatedWithPossibleDelta() {
        return allocated.get();
    }

    // releasing the memory back to the OS, freeing the block, an opposite of allocation, not thread safe
    void clean() {
        UnsafeUtils.unsafe.freeMemory(memAddress);
    }

    long getMemAddress() {
        return memAddress;
    }

    // how many bytes a block may include, regardless allocated/free
    int getCapacity() {
        return capacity;
    }

}
