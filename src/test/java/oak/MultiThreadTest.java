package oak;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MultiThreadTest {

    private OakMapOffHeapImpl oak;
    private final int NUM_THREADS = 20;
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

            for (int i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                OakBuffer buffer = oak.get(bb);
                assertTrue(buffer == null);
            }
            for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }
            for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                OakBuffer buffer = oak.get(bb);
                assertTrue(buffer != null);
                assertEquals(i, buffer.getInt(0));
            }
            for (int i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                OakBuffer buffer = oak.get(bb);
                assertTrue(buffer == null);
            }
            for (int i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }
            for (int i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }
            for (int i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                OakBuffer buffer = oak.get(bb);
                assertTrue(buffer != null);
                assertEquals(i, buffer.getInt(0));
            }
            for (int i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }
            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                OakBuffer buffer = oak.get(bb);
                assertTrue(buffer == null);
            }
            for (int i = 3 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            Iterator valIter = oak.valuesIterator();
            int c = (int) Math.round(0.5 * maxItemsPerChunk);
            while (valIter.hasNext()) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(c);
                bb.flip();
                OakBuffer buffer = oak.get(bb);
                assertTrue(buffer != null);
                assertEquals(c, buffer.getInt(0));
                assertEquals(c, ((OakBuffer) (valIter.next())).getInt(0));
                c++;
                if (c == 2 * maxItemsPerChunk) {
                    break;
                }
            }
            assertEquals(2 * maxItemsPerChunk, c);

            ByteBuffer from = ByteBuffer.allocate(4);
            from.putInt(0);
            from.flip();
            ByteBuffer to = ByteBuffer.allocate(4);
            to.putInt(2 * maxItemsPerChunk);
            to.flip();
            OakMap sub = oak.subMap(from, true, to, false);
            valIter = sub.valuesIterator();
            c = (int) Math.round(0.5 * maxItemsPerChunk);
            while (valIter.hasNext()) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(c);
                bb.flip();
                OakBuffer buffer = oak.get(bb);
                assertTrue(buffer != null);
                assertEquals(c, buffer.getInt(0));
                assertEquals(c, ((OakBuffer) (valIter.next())).getInt(0));
                c++;
            }
            assertEquals(2 * maxItemsPerChunk, c);

            from = ByteBuffer.allocate(4);
            from.putInt(1);
            from.flip();
            to = ByteBuffer.allocate(4);
            to.putInt((int) Math.round(0.5 * maxItemsPerChunk));
            to.flip();
            sub = oak.subMap(from, true, to, false);
            valIter = sub.valuesIterator();
            assertFalse(valIter.hasNext());

            from = ByteBuffer.allocate(4);
            from.putInt(4 * maxItemsPerChunk);
            from.flip();
            to = ByteBuffer.allocate(4);
            to.putInt(5 * maxItemsPerChunk);
            to.flip();
            sub = oak.subMap(from, true, to, false);
            valIter = sub.valuesIterator();
            assertFalse(valIter.hasNext());

            for (int i = (int) Math.round(0.5 * maxItemsPerChunk); i < maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                ByteBuffer bb1 = ByteBuffer.allocate(4);
                bb1.putInt(i + 1);
                bb1.flip();
                oak.putIfAbsent(bb, bb1);
            }


        }
    }

    @Test
    public void testThreads() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadTest.RunThreads(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        for (int i = (int) Math.round(0.5 * maxItemsPerChunk); i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }
        for (int i = 2 * maxItemsPerChunk; i < 4 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        for (int i = 0; i < (int) Math.round(0.5 * maxItemsPerChunk); i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
    }

    class RunThreadsDescend implements Runnable {
        CountDownLatch latch;

        RunThreadsDescend(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer i;
            Iterator iter;
            ByteBuffer bb;

            for (i = 0; i < 6 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            iter = oak.valuesIterator();
            i = 0;
            while (iter.hasNext()) {
                OakBuffer buffer = ((OakBuffer) (iter.next()));
                if (buffer == null) {
                    continue;
                }
                try {
                    i = buffer.getInt(0);
                } catch (NullPointerException exp) {
                }

            }
            Assert.assertTrue(i > maxItemsPerChunk);
            iter = oak.descendingMap().valuesIterator();
            while (iter.hasNext()) {
                OakBuffer buffer = ((OakBuffer) (iter.next()));
                if (buffer == null) {
                    continue;
                }
                try {
                    i = buffer.getInt(0);
                } catch (NullPointerException exp) {
                }
            }
            assertEquals(0, i.intValue());

            for (i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }
            iter = oak.valuesIterator();
            i = 0;
            while (iter.hasNext()) {
                OakBuffer buffer = ((OakBuffer) (iter.next()));
                if (buffer == null) {
                    continue;
                }
                try {
                    i = buffer.getInt(0);
                } catch (NullPointerException exp) {
                }
            }
            Assert.assertTrue(i > maxItemsPerChunk);
            iter = oak.descendingMap().valuesIterator();
            while (iter.hasNext()) {
                OakBuffer buffer = ((OakBuffer) (iter.next()));
                if (buffer == null) {
                    continue;
                }
                try {
                    i = buffer.getInt(0);
                } catch (NullPointerException exp) {
                }
            }
            assertEquals(0, i.intValue());

            Consumer<WritableOakBuffer> func = buffer -> {
                if (buffer.getInt(0) == 0) {
                    buffer.putInt(0);
                    buffer.putInt(1);
                }
            };

            for (i = 2 * maxItemsPerChunk; i < 3 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.computeIfPresent(bb, func);
            }

            for (i = 5 * maxItemsPerChunk; i < 6 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }
            iter = oak.valuesIterator();
            i = 0;
            while (iter.hasNext()) {
                OakBuffer buffer = ((OakBuffer) (iter.next()));
                if (buffer == null) {
                    continue;
                }
                try {
                    i = buffer.getInt(0);
                } catch (NullPointerException exp) {
                }
            }
            Assert.assertTrue(i > maxItemsPerChunk);
            iter = oak.descendingMap().valuesIterator();
            while (iter.hasNext()) {
                OakBuffer buffer = ((OakBuffer) (iter.next()));
                if (buffer == null) {
                    continue;
                }
                try {
                    i = buffer.getInt(0);
                } catch (NullPointerException exp) {
                }
            }
            assertEquals(0, i.intValue());

            for (i = 0; i < 6 * maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (i = 0; i < maxItemsPerChunk; i++) {
                bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.computeIfPresent(bb, func);
            }

        }
    }

    @Test
    public void testThreadsDescend() throws InterruptedException {
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new MultiThreadTest.RunThreadsDescend(latch)));
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
            if (i == 0) {
                assertEquals(1, buffer.getInt(4));
            }
        }
    }
}
