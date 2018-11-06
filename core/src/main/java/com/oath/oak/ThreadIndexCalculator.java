
package com.oath.oak;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadIndexCalculator {

    private final AtomicInteger nextFreeIndex;
    private final ConcurrentMap<Long, Integer> map;
    static final int MAX_THREADS = 32;

    private ThreadIndexCalculator() {
        this.nextFreeIndex = new AtomicInteger(0);
        map = new ConcurrentHashMap<>();
    }


    public int getIndex() {
        long tid = Thread.currentThread().getId();
        Integer index = map.get(tid);
        if (index != null) {
            return index;
        }
        int newIndex = nextFreeIndex.getAndIncrement();
        assert (newIndex < MAX_THREADS);
        map.put(tid, newIndex);
        return newIndex;
    }

    public static ThreadIndexCalculator newInstance() {
        return new ThreadIndexCalculator();
    }
}
