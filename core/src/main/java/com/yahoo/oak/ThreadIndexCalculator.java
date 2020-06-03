/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.util.NoSuchElementException;

final class ThreadIndexCalculator {

    public static final int MAX_THREADS = 64;
    private static final int INVALID_THREAD_ID = -1;
    // Long for correctness and anti false-sharing
    private final long[] indices = new long[MAX_THREADS];

    private ThreadIndexCalculator() {
        for (int i = 0; i < MAX_THREADS; ++i) {
            indices[i] = INVALID_THREAD_ID;
        }
    }

    private int getExistingIndex(long threadID) {
        int iterationCnt = 0;
        int currentIndex = ((int) threadID) % MAX_THREADS;
        while (indices[currentIndex] != threadID) {
            if (indices[currentIndex] == INVALID_THREAD_ID) {
                // negative output indicates that a new index need to be created for this thread id
                return -1 * currentIndex;
            }
            currentIndex = (currentIndex + 1) % MAX_THREADS;
            iterationCnt++;
            assert iterationCnt < MAX_THREADS;
        }
        return currentIndex;
    }

    public int getIndex() {
        long tid = Thread.currentThread().getId();
        int threadIdx = getExistingIndex(tid);
        if (threadIdx > 0) {
            return threadIdx;
        }
        if (threadIdx == 0) {
            // due to multiplying by -1 check this special array element
            if (tid == indices[0]) {
                return threadIdx;
            }
        }
        int i = threadIdx * -1;
        while (!UnsafeUtils.unsafe.compareAndSwapLong(indices,
                Unsafe.ARRAY_LONG_BASE_OFFSET + i * Unsafe.ARRAY_LONG_INDEX_SCALE, INVALID_THREAD_ID, tid)) {
            //TODO get out of loop sometime
            i = (i + 1) % MAX_THREADS;
        }
        return i;
    }

    public void releaseIndex() {
        long tid = Thread.currentThread().getId();
        int index = getExistingIndex(tid);
        if (index < 0) {
            // There is no such thread index in the calculator, so throw NoSuchElementException
            // Probably releasing the same thread twice
            throw new NoSuchElementException();
        }
        indices[index] = INVALID_THREAD_ID;
    }

    public static ThreadIndexCalculator newInstance() {
        return new ThreadIndexCalculator();
    }
}
