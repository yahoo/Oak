package oak;

import oak.IntComparator;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MultiThreadComputeTest {

    private OakMapOnHeapImpl oak;
    private final int NUM_THREADS = 20;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<WritableOakBuffer> func;

    @Before
    public void init() {
        Comparator<ByteBuffer> comparator = new IntComparator();
        ByteBuffer min = ByteBuffer.allocate(10);
        min.putInt(Integer.MIN_VALUE);
        min.flip();
        oak = new OakMapOnHeapImpl(comparator, min);
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
        func = buffer -> {
            if (buffer.getInt(0) == 0) {
                buffer.putInt(0, 1);
            }
        };
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

            ByteBuffer bb;

            for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }

            bb = ByteBuffer.allocate(4);
            bb.putInt(0);
            bb.flip();
            OakBuffer buffer = oak.getHandle(bb);
            assertTrue(buffer != null);

            bb.putInt(0, Chunk.MAX_ITEMS);
            OakBuffer delBuffer = oak.getHandle(bb);
            boolean check = false;
            if (delBuffer != null) check = true;

            for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            bb.putInt(0, 1);
            ByteBuffer bb2 = ByteBuffer.allocate(4);
            bb2.putInt(2);
            bb2.flip();
            oak.put(bb, bb2);

            for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.computeIfPresent(bb, func);
            }

            assertEquals(1, buffer.getInt(0));

            for (int i = Chunk.MAX_ITEMS; i < 2 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            if (check) {
                try {
                    delBuffer.getInt(0);
                } catch (NullPointerException ex) {
                    check = false;
                    bb = ByteBuffer.allocate(4);
                    bb.putInt(Chunk.MAX_ITEMS);
                    bb.flip();
                    assertTrue(oak.getHandle(bb) == null);
                }
            }
            assertFalse(check);

            for (int i = 5 * Chunk.MAX_ITEMS; i < 6 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (int i = 5 * Chunk.MAX_ITEMS; i < 6 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 2 * Chunk.MAX_ITEMS; i < 3 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }

            for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = Chunk.MAX_ITEMS; i < 2 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 4 * Chunk.MAX_ITEMS; i < 6 * Chunk.MAX_ITEMS; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

        }
    }

    @Test
    public void testThreadsCompute() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadComputeTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        for (int i = 0; i < Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.getHandle(bb);
            assertTrue(buffer != null);
            if (i == 0) {
                assertEquals(1, buffer.getInt(0));
                continue;
            }
            if (i == 1) {
                assertEquals(2, buffer.getInt(0));
                continue;
            }
            assertEquals(i, buffer.getInt(0));
        }
        for (int i = Chunk.MAX_ITEMS; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.getHandle(bb);
            assertTrue(buffer == null);
        }
        for (int i = 2 * Chunk.MAX_ITEMS; i < 3 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.getHandle(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }
        for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.getHandle(bb);
            assertTrue(buffer == null);
        }

        for (int i = 4 * Chunk.MAX_ITEMS; i < 6 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.getHandle(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }

    }


}
