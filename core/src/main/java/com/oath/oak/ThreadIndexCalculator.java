
package com.oath.oak;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadIndexCalculator {

    private final ConcurrentMap<Long, Integer> map;
    static final int MAX_THREADS = 32;
    private final ConcurrentLinkedQueue<Integer> freeIndexList;


    private ThreadIndexCalculator() {
        map = new ConcurrentHashMap<>();
        freeIndexList = new ConcurrentLinkedQueue<>();
        for (Integer i=0; i < MAX_THREADS; ++i) {
            freeIndexList.add(i);
        }
    }


    public int getIndex() {
        long tid = Thread.currentThread().getId();
        Integer index = map.get(tid);
        if (index != null) {
            return index;
        }
        Integer newIndex = freeIndexList.poll();
        assert newIndex != null;
        map.put(tid, newIndex);
        return newIndex;
    }

    public void releaseIndex() {
        long tid = Thread.currentThread().getId();
        Integer index = map.remove(tid);
        assert index != null;
        freeIndexList.add(index);
    }


    public static ThreadIndexCalculator newInstance() {
        return new ThreadIndexCalculator();
    }
}
