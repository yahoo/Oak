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

public class MemoryManager {
    private final OakMemoryAllocator keysMemoryAllocator;
    private final OakMemoryAllocator memoryAllocator;
    private final AtomicLong[] timeStamps;
    private final ArrayList<LinkedList<Pair<Long, ByteBuffer>>> releasedArray;
    private final AtomicLong max;

    private static final int RELEASES_DEFAULT = 100; // TODO: make it configurable
    private final ThreadIndexCalculator threadIndexCalculator;

    private int releases; // to be able to change it for testing

    public MemoryManager(OakMemoryAllocator ma, ThreadIndexCalculator threadIndexCalculator) {
        assert ma != null;

        this.memoryAllocator = ma;
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
        keysMemoryAllocator = new DirectMemoryAllocator();
    }

    public ByteBuffer allocate(int size) {
        return memoryAllocator.allocate(size);
    }

    public void close() {
        memoryAllocator.close();
        keysMemoryAllocator.close();
    }

    void release(ByteBuffer bb) {
        memoryAllocator.free(bb);
    }

    // the MSB (busy bit) is not set
    private boolean isIdle(long timeStamp) {
        return (timeStamp) == 0L;
    }

    public void startOperation() {

    }

    public void stopOperation() {

    }


    public void assertIfNotIdle() {

    }

    // how many memory is allocated for this OakMap
    public long allocated() {
        return memoryAllocator.allocated();
    }

    // used only for testing
    void setGCtrigger(int i) {
        releases = i;
    }

    public ByteBuffer allocateKeys(int bytes) {
        return keysMemoryAllocator.allocate(bytes);
    }

    public void releaseKeys(ByteBuffer keys) {
        keysMemoryAllocator.free(keys);
    }
}
