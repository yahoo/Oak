
package com.oath.oak;

import java.util.concurrent.atomic.AtomicLong;

public class ThreadIndexCalculator {

    public static final int MAX_THREADS = 32;
    private static final int INVALID_THREAD_ID = -1;
    // Long for correctness and anti false-sharing
    private AtomicLong[] indices = new AtomicLong[MAX_THREADS];

    private ThreadIndexCalculator() {
        for (int i = 0; i < MAX_THREADS; ++i) {
            indices[i] = new AtomicLong(INVALID_THREAD_ID);
        }
    }

    private int getExistingIndex(long threadID){
      int iterationCnt = 0;
      int currentIndex = ((int)threadID) % MAX_THREADS;
      long currentThreadID = indices[currentIndex].get();
      while (currentThreadID != threadID) {
        if (currentThreadID == INVALID_THREAD_ID) {
          return -1*currentIndex;
        }
        currentIndex = (currentIndex + 1) % MAX_THREADS;
        currentThreadID = indices[currentIndex].get();
        iterationCnt++;
        assert iterationCnt<MAX_THREADS;
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
          if (tid == indices[0].get()) {
            return threadIdx;
          }
        }
        int i = threadIdx*-1;
        while (!indices[i].compareAndSet(INVALID_THREAD_ID, tid)) {
            //TODO get out of loop sometime
            i = (i + 1) % MAX_THREADS;
        }
        return i;
    }

    public void releaseIndex() {
        long tid = Thread.currentThread().getId();
        int index = getExistingIndex(tid);
        assert index >=0 ;
        indices[index].set(INVALID_THREAD_ID);
    }

    public static ThreadIndexCalculator newInstance() {
        return new ThreadIndexCalculator();
    }
}
