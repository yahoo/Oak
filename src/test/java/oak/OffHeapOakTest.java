package oak;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class OffHeapOakTest {
    private OakMapOffHeapImpl oak;
    private final int NUM_THREADS = 12;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<WritableOakBuffer> emptyFunc;

    @Before
    public void init() {
        oak = new OakMapOffHeapImpl();
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
        emptyFunc = buffer -> {
            ByteBuffer bb = buffer.getByteBuffer();
        };
    }

    @Test
    public void testPutIfAbsent() {
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
    }

    @Test
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new OffHeapOakTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        for (int i = 0; i < 6 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }
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

            for (int i = 0; i < 6 * Chunk.MAX_ITEMS; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }

            for (int i = 0; i < 6 * Chunk.MAX_ITEMS; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 0; i < 6 * Chunk.MAX_ITEMS; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (int i = 0; i < 6 * Chunk.MAX_ITEMS; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 0; i < 6 * Chunk.MAX_ITEMS; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }

            for (int i = 0; i < Chunk.MAX_ITEMS; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsentComputeIfPresent(bb, () -> bb, emptyFunc);
            }

        }
    }
}
