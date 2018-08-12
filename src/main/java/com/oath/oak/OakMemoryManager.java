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

class OakMemoryManager { // TODO interface allocate, release

    final MemoryPool pool;
    final AtomicLong[] timeStamps;
    final ArrayList<LinkedList<Triplet>> releasedArray;
    final AtomicLong max;

    // Pay attention, this busy bit is used as a counter for nested calls of start thread method.
    // It can be increased only 2^23 (8,388,608) times. This is needed for iterators that reappear
    // in the data structure and assume the chunks on their way are not coing to be de-allocated.
    // In case iterator should cover more than this number of items, the code should be adopted.
    // The stop thread method (in turn) decreases the busy bit count and should achieve zero when
    // thread is really idle.
    private static final long BUSY_BIT = 1L << 48;
    private static final long IDLE_MASK = 0xFFFF000000000000L; // last byte
    static final int RELEASES = 100;

    OakMemoryManager(MemoryPool pool) {
        this.pool = pool;
        this.timeStamps = new AtomicLong[Chunk.MAX_THREADS];
        for (int i = 0; i < Chunk.MAX_THREADS; i++) {
            this.timeStamps[i] = new AtomicLong(0);
        }
        this.releasedArray = new ArrayList<>(Chunk.MAX_THREADS);
        for (int i = 0; i < Chunk.MAX_THREADS; i++) {
            releasedArray.add(i, new LinkedList<>());
        }
        max = new AtomicLong();
    }

    Pair<Integer, ByteBuffer> allocate(int capacity) {
        return pool.allocate(capacity);
    }

    static class Triplet {

        long max;
        int i;
        ByteBuffer bb;

        Triplet(long max, int i, ByteBuffer bb) {
            this.max = max;
            this.i = i;
            this.bb = bb;
        }

    }

    private boolean assertDoubleRelease(ByteBuffer bb) {
        for (int i = 0; i < Chunk.MAX_THREADS; i++) {
            LinkedList<Triplet> list = releasedArray.get(i);
            for (Triplet t : list
                    ) {
                if (t.bb == bb)
                    return true;
            }
        }
        return false;
    }

    void release(int i, ByteBuffer bb) {
//        assert !assertDoubleRelease(bb);
        int idx = InternalOakMap.getThreadIndex();
        LinkedList<Triplet> myList = releasedArray.get(idx);
        myList.addFirst(new Triplet(this.max.get(), i, bb));
//        myList.addFirst(new Triplet(-1, i, bb));
        checkRelease(idx, myList);
    }

    private void checkRelease(int idx, LinkedList<Triplet> myList) {
        if (myList.size() >= RELEASES) {
            forceRelease(myList);
        }
    }


    void forceRelease(LinkedList<Triplet> myList) {
        long min = Long.MAX_VALUE;
        for (int j = 0; j < Chunk.MAX_THREADS; j++) {
            long timeStamp = timeStamps[j].get();
            if (!isIdle(timeStamp)) {
                min = Math.min(min, getValue(timeStamp));
            }
        }
        for (int i = 0; i < myList.size(); i++) {
            Triplet triplet = myList.get(i);
//            if (triplet.max == -1) {
//                continue;
//            }
            if (triplet.max < min) {
                myList.remove(triplet);
                pool.free(triplet.i, triplet.bb);
            }
        }
    }

    // the MSB (busy bit) is not set
    boolean isIdle(long timeStamp) {
        return (timeStamp & IDLE_MASK) == 0L;
    }

    long getValue(long timeStamp) {
        return timeStamp & (~IDLE_MASK);
    }

    void startThread() {
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
        // so the number is always growing
        long global_max = this.max.get();
        long diff = (v > global_max) ? v - global_max + 1 : 1;
        long max = this.max.addAndGet(diff);

        l &= IDLE_MASK; // zero the local counter
        l += max;
        l += BUSY_BIT; // set to not idle
        timeStamp.set(l);
        assert !isIdle(timeStamp.get());
    }

    void stopThread() {
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
}