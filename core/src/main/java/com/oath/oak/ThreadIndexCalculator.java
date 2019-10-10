
package com.oath.oak;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadIndexCalculator {

    public static final int MAX_THREADS = 32;
    private static final int INVALID_THREAD_ID = -1;
    private ThreadLocal<Integer> local = ThreadLocal.withInitial(() -> INVALID_THREAD_ID);
    private AtomicInteger[] indices = new AtomicInteger[MAX_THREADS];

    private ThreadIndexCalculator() {
        for (int i = 0; i < MAX_THREADS; ++i) {
            indices[i] = new AtomicInteger(INVALID_THREAD_ID);
        }
    }


    public int getIndex() {

        int localInt = local.get();
        if (localInt != INVALID_THREAD_ID) {
            return localInt;
        }
        int tid = (int) Thread.currentThread().getId();
        int i = tid % MAX_THREADS;
        while (!indices[i].compareAndSet(INVALID_THREAD_ID, tid)) {
            //TODO get out of loop sometime
            i = (i + 1) % MAX_THREADS;
        }
        local.set(i);
        return i;
    }

    public void releaseIndex() {
        indices[local.get()].set(INVALID_THREAD_ID);
        local.set(INVALID_THREAD_ID);
    }

    public static ThreadIndexCalculator newInstance() {
        return new ThreadIndexCalculator();
    }
}
