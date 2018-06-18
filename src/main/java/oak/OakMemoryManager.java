package oak;

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

    private static final long IDLE_BIT = 1L << 56;
    private static final long IDLE_MASK = 0xFF00000000000000L; // last byte
    static final int RELEASES = 100;

    OakMemoryManager(MemoryPool pool) {
        this.pool = pool;
        this.timeStamps = new AtomicLong[Chunk.MAX_THREADS];
        for (int i = 0; i < Chunk.MAX_THREADS; i++) {
            this.timeStamps[i] = new AtomicLong();
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
        int idx = OakMapOffHeapImpl.getThreadIndex();
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

    boolean isIdle(long timeStamp) {
        return (timeStamp & IDLE_MASK) == 0L;
    }

    long getValue(long timeStamp) {
        return timeStamp & (~IDLE_MASK);
    }

    void startThread() {
        int idx = OakMapOffHeapImpl.getThreadIndex();
        AtomicLong timeStamp = timeStamps[idx];
        long l = timeStamp.get();

        if (!isIdle(l)) {
            // if already not idle, don't increase timestamp
            l += IDLE_BIT; // just increment idle
            timeStamp.set(l);
            return;
        }

        long max = this.max.incrementAndGet();
        l &= IDLE_MASK;
        l += max;
        l += IDLE_BIT; // set to not idle
        timeStamp.set(l);
    }

    void stopThread() {
        int idx = OakMapOffHeapImpl.getThreadIndex();
        AtomicLong timeStamp = timeStamps[idx];
        long l = timeStamp.get();
        assert !isIdle(l);
        l -= IDLE_BIT; // set to idle
        timeStamp.set(l);
    }

}