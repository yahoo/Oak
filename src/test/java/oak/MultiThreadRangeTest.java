package oak;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

public class MultiThreadRangeTest {

    private OakMapOffHeapImpl oak;
    private final int NUM_THREADS = 1;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = 100;

    @Before
    public void init() {
        Comparator<Object> comparator = new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                ByteBuffer bb1 = (ByteBuffer) o1;
                ByteBuffer bb2 = (ByteBuffer) o2;
                int i1 = bb1.getInt(bb1.position());
                int i2 = bb2.getInt(bb2.position());
                if (i1 > i2) {
                    return 1;
                } else if (i1 < i2) {
                    return -1;
                } else {
                    return 0;
                }
            }
        };
        ByteBuffer min = ByteBuffer.allocate(10);
        min.putInt(Integer.MIN_VALUE);
        min.flip();
        oak = new OakMapOffHeapImpl(comparator, min, maxItemsPerChunk, maxBytesPerChunkItem);
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
    }

    class RunThreads implements Runnable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Random r = new Random();

            ByteBuffer from = ByteBuffer.allocate(4);
            from.putInt(r.nextInt(10 * maxItemsPerChunk));
            from.flip();
            Iterator valIter = oak.tailMap(from, true).valuesIterator();
            int i = 0;
            while (valIter.hasNext() && i < 100) {
                valIter.next();
                i++;
            }

        }
    }

    @Test
    public void testRange() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadRangeTest.RunThreads(latch)));
        }

        // fill
        Random r = new Random();
        for (int i = 5 * maxItemsPerChunk; i > 0; ) {
            Integer j = r.nextInt(10 * maxItemsPerChunk);
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(j);
            bb.flip();
            if (oak.putIfAbsent(bb, bb)) i--;
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        int size = 0;
        for (int i = 0; i < 10 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            if (buffer != null) size++;
        }
        assertEquals(5 * maxItemsPerChunk, size);
    }

}
