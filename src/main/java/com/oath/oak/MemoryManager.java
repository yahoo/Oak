/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

class MemoryManager {

    final OakMemoryAllocator memoryAllocator;
    final AtomicLong[] timeStamps;
    final ArrayList<LinkedList<Pair<Long,ByteBuffer>>> releasedArray;
    final AtomicLong max;

    // Pay attention, this busy bit is used as a counter for nested calls of start thread method.
    // It can be increased only 2^23 (8,388,608) times, before overflowing.
    // This is needed for iterators that reappear
    // in the data structure and assume the chunks on their way are not going to be de-allocated.
    // In case iterator should cover more than this number of items, the code should be adopted.
    // The stop thread method (in turn) decreases the busy bit count and should achieve zero when
    // thread is really idle.
    private static final long BUSY_BIT = 1L << 48;
    private static final long IDLE_MASK = 0xFFFF000000000000L; // last byte
    private static final int RELEASES_DEFAULT = 100; // TODO: make it configurable

    private int releases; // to be able to change it for testing

    MemoryManager(long capacity, OakMemoryAllocator ma) {
        this.memoryAllocator = (ma == null) ? new OakNativeMemoryAllocator(capacity) : ma;
        this.timeStamps = new AtomicLong[Chunk.MAX_THREADS];
        for (int i = 0; i < Chunk.MAX_THREADS; i++) {
            this.timeStamps[i] = new AtomicLong(0);
        }
        this.releasedArray = new ArrayList<>(Chunk.MAX_THREADS);
        for (int i = 0; i < Chunk.MAX_THREADS; i++) {
            releasedArray.add(i, new LinkedList<>());
        }
        max = new AtomicLong(0);
        releases = RELEASES_DEFAULT;
    }

    ByteBuffer allocate(int size) {
        return memoryAllocator.allocate(size);
    }

    void close() {
        memoryAllocator.close();
    }

    private boolean assertDoubleRelease(ByteBuffer bb) {
        for (int i = 0; i < Chunk.MAX_THREADS; i++) {
            LinkedList<Pair<Long,ByteBuffer>> list = releasedArray.get(i);
            for (Pair<Long,ByteBuffer> p : list
                    ) {
                if (p.getValue() == bb)
                    return true;
            }
        }
        return false;
    }

    void release(ByteBuffer bb) {
//        assert !assertDoubleRelease(bb);
        int idx = InternalOakMap.getThreadIndex();
        LinkedList<Pair<Long,ByteBuffer>> myList = releasedArray.get(idx);
        myList.addFirst(new Pair<Long,ByteBuffer>(this.max.incrementAndGet(), bb));
        checkRelease(idx, myList);
    }

    private void checkRelease(int idx, LinkedList<Pair<Long,ByteBuffer>> myList) {
        if (myList.size() >= releases) {
            forceRelease(myList);
        }
    }


    private void forceRelease(LinkedList<Pair<Long,ByteBuffer>> myList) {
        long min = Long.MAX_VALUE;
        for (int j = 0; j < Chunk.MAX_THREADS; j++) {
            long timeStamp = timeStamps[j].get();
            if (!isIdle(timeStamp)) {
                // find minimal timestam among the working threads
                min = Math.min(min, getValue(timeStamp));
            }
        }
        // collect and remove in two steps to avoid concurrent modification exception
        LinkedList<Pair<Long,ByteBuffer>> toBeRemovedList = new LinkedList<>();
        for (Pair<Long,ByteBuffer> pair : myList ) {
            // pair's key is the "old max", meaning the timestamp when the ByteBuffer was released
            // (disconnected from the data structure)
            if (pair.getKey() /* max */ < min) {
                toBeRemovedList.add(pair);
            }
        }
        myList.removeAll(toBeRemovedList);
        for (Pair<Long,ByteBuffer> pair : toBeRemovedList ) {
            memoryAllocator.free(pair.getValue() /* pair's value is the byte buffer */);
        }
    }

    // the MSB (busy bit) is not set
    boolean isIdle(long timeStamp) {
        return (timeStamp & IDLE_MASK) == 0L;
    }

    long getValue(long timeStamp) {
        return timeStamp & (~IDLE_MASK);
    }

    void startOperation() {
        int idx = InternalOakMap.getThreadIndex();
        AtomicLong timeStamp = timeStamps[idx];
        long l = timeStamp.get();
        long v = getValue(l);

        if (!isIdle(l)) {
            // if already not idle, busy bit is set, increase just internal counter, not global max
            l += 1;
            l += BUSY_BIT; // just increment in one busy bit, that serves as a counter
            timeStamp.set(l);
            assert !isIdle(timeStamp.get()); // the thread should continue to be marked as busy
            return;
        }

        // if our local counter v overgrown the global max, return the global max to be maximum
        // so the number (per thread) is always growing
        long global_max = this.max.get();
        long diff = (v > global_max) ? v - global_max + 1 : 1;
        long max = this.max.addAndGet(diff);

        l &= IDLE_MASK; // zero the local counter
        l += max;       // set it to be current maximum
        l += BUSY_BIT;  // set to not idle
        timeStamp.set(l);
        assert !isIdle(timeStamp.get());
    }

    void stopOperation() {
        int idx = InternalOakMap.getThreadIndex();
        AtomicLong timeStamp = timeStamps[idx];
        long l = timeStamp.get();
        assert !isIdle(l);
        l -= BUSY_BIT; // set to idle (in case this is the last nested call)
        timeStamp.set(l);
    }

    void assertIfNotIdle() {
        int idx = InternalOakMap.getThreadIndex();
        AtomicLong timeStamp = timeStamps[idx];
        long l = timeStamp.get();
        assert isIdle(l);
    }

    // how many memory is allocated for this OakMap
    long allocated() { return memoryAllocator.allocated(); }

    // used only for testing
    OakMemoryAllocator getMemoryAllocator() {
        return memoryAllocator;
    }

    // used only for testing
    void setGCtrigger(int i) { releases = i; }
}