
package com.oath.oak;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadIndexCalculator {

    public static final int MAX_THREADS = 32;
    private ThreadLocal<Integer> local = ThreadLocal.withInitial(() -> -1);
    private AtomicInteger[] indices = new AtomicInteger[MAX_THREADS];

    private ThreadIndexCalculator() {
        for (int i = 0; i < MAX_THREADS; ++i) {
            indices[i] = new AtomicInteger(-1);
        }
    }


    public int getIndex() {

        int localInt = local.get();
        if (localInt != -1) {
            return localInt;
        }
        int tid = (int) Thread.currentThread().getId();
        int i = tid % MAX_THREADS;
        while (!indices[i].compareAndSet(-1, tid)) {
            //TODO get out of loop sometime
            i = (i + 1) % MAX_THREADS;
        }
        local.set(i);
        return i;
    }

    public void releaseIndex() {
        indices[local.get()].set(-1);
        local.set(-1);
    }

    public static ThreadIndexCalculator newInstance() {
        return new ThreadIndexCalculator();
    }
}
