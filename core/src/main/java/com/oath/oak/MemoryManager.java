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
    private final AtomicLong[] timeStamps;
    final ArrayList<LinkedList<Pair<Long,ByteBuffer>>> releasedArray;
    private final AtomicLong max;

    private static final int RELEASES_DEFAULT = 100; // TODO: make it configurable
    private final ThreadIndexCalculator threadIndexCalculator;

    private int releases; // to be able to change it for testing

    MemoryManager(long capacity, OakMemoryAllocator ma, ThreadIndexCalculator threadIndexCalculator) {
        this.memoryAllocator = (ma == null) ? new OakNativeMemoryAllocator(capacity) : ma;
        this.timeStamps = new AtomicLong[ThreadIndexCalculator.MAX_THREADS];
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.timeStamps[i] = new AtomicLong(0);
        }
        this.releasedArray = new ArrayList<>(ThreadIndexCalculator.MAX_THREADS);
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            releasedArray.add(i, new LinkedList<>());
        }
        max = new AtomicLong(0);
        releases = RELEASES_DEFAULT;
        this.threadIndexCalculator = threadIndexCalculator;
    }

    ByteBuffer allocate(int size) {
        return memoryAllocator.allocate(size);
    }

    void close() {
        releasedArray.forEach(this::forceRelease);
        memoryAllocator.close();
    }

    private boolean assertDoubleRelease(ByteBuffer bb) {
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
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
        int idx = threadIndexCalculator.getIndex();
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
        for (int j = 0; j < ThreadIndexCalculator.MAX_THREADS; j++) {
            long timeStamp = timeStamps[j].get();
            if (!isIdle(timeStamp)) {
                // find minimal timestamp among the working threads
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
    private boolean isIdle(long timeStamp) {
        return (timeStamp) == 0L;
    }

    private long getValue(long timeStamp) {
        return timeStamp;
    }

    long getandIncrementEpoch(){
        return max.getAndIncrement();
    }

    void startOperation() {
        int idx = threadIndexCalculator.getIndex();
        AtomicLong timeStamp = timeStamps[idx];
        timeStamp.set(max.getAndIncrement());
    }

    void stopOperation() {
        int idx = threadIndexCalculator.getIndex();
        AtomicLong timeStamp = timeStamps[idx];
        timeStamp.set(0);
        threadIndexCalculator.releaseIndex();
    }

    void iteratorStartOperation(long epoch) {
        int idx = threadIndexCalculator.getIndex();
        AtomicLong timeStamp = timeStamps[idx];
        timeStamp.set(epoch);
        return;
    }

    void iteratorStopOperation() {
        int idx = threadIndexCalculator.getIndex();
        AtomicLong timeStamp = timeStamps[idx];
        timeStamp.set(0);
    }

    void assertIfNotIdle() {
        int idx = threadIndexCalculator.getIndex();
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
