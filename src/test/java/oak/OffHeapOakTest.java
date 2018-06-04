package oak;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import oak.OakMap.KeyInfo;

public class OffHeapOakTest {
    private OakMapOffHeapImpl oak;
    private final int NUM_THREADS = 12;
    private ArrayList<Thread> threads;
    private CountDownLatch latch;
    private Consumer<WritableOakBuffer> emptyFunc;
    int maxItemsPerChunk = 2048;
    int maxBytesPerChunkItem = 100;

    @Before
    public void init() {
        oak = new OakMapOffHeapImpl(maxItemsPerChunk, maxBytesPerChunkItem);
        latch = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
        emptyFunc = buffer -> {
            ByteBuffer bb = buffer.getByteBuffer();
        };
    }

    @Test
    public void testPutIfAbsent() {
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (int i = 0; i < 2 * maxItemsPerChunk; i++) {
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

        for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
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

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsent(bb, bb);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }

            for (int i = 0; i < 6 * maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.put(bb, bb);
            }

            for (int i = 0; i < maxItemsPerChunk; i++) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.putIfAbsentComputeIfPresent(bb, new KeyCreator(), buff -> 4, new ValueCreator(i), 4, emptyFunc);
                oak.putIfAbsentComputeIfPresent(bb, () -> bb, emptyFunc);
            }

        }
    }

    public class ValueCreator implements Consumer<ByteBuffer>  {

        int i;

        public ValueCreator(int i) {
            this.i = i;
        }

        @Override
        public void accept(ByteBuffer byteBuffer) {
            byteBuffer.putInt(i);
            byteBuffer.flip();
        }
    }

    public class KeyCreator implements Consumer<KeyInfo> {
        @Override
        public void accept(KeyInfo keyInfo) {
            ByteBuffer key = (ByteBuffer) keyInfo.key;
            ByteBuffer buffer = keyInfo.buffer;
            int position = buffer.position() + keyInfo.index;
            buffer.putInt(position, key.getInt(0));
        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPutIfAbsentComputeIfPresentWithValueCreator() {
        Consumer<WritableOakBuffer> func = writableOakBuffer -> {
            if (writableOakBuffer.getInt() == 0) writableOakBuffer.putInt(0, 1);
        };
        ByteBuffer key = ByteBuffer.allocate(4);
        key.putInt(0);
        key.flip();
        assertFalse(oak.computeIfPresent(key, func));

        oak.putIfAbsentComputeIfPresent(key, new KeyCreator(), buff -> 4, new ValueCreator(0), 4, func);
        OakBuffer buffer = oak.get(key);
        assertTrue(buffer != null);
        assertEquals(0, buffer.getInt(0));
        oak.putIfAbsentComputeIfPresent(key, new KeyCreator(), buff -> 4, new ValueCreator(0), 4, func);
        buffer = oak.get(key);
        assertTrue(buffer != null);
        assertEquals(1, buffer.getInt(0));
        assertEquals(4, buffer.remaining());
        ByteBuffer two = ByteBuffer.allocate(4);
        two.putInt(2);
        two.flip();
        oak.put(key, two);
        assertEquals(4, buffer.remaining());
        assertEquals(2, buffer.getInt(0));
        assertTrue(oak.computeIfPresent(key, func));
        assertEquals(4, buffer.remaining());
        assertEquals(2, buffer.getInt(0));
        Consumer<WritableOakBuffer> func2 = writableOakBuffer -> {
            if (writableOakBuffer.getInt() == 0) {
                writableOakBuffer.putInt(0, 0);
                writableOakBuffer.putInt(1);
            }
        };
        oak.put(key, key);
        oak.putIfAbsentComputeIfPresent(key, new KeyCreator(), buff -> 4, new ValueCreator(0),4, func2);
        assertEquals(8, buffer.remaining());
        assertEquals(0, buffer.getInt(0));
        assertEquals(1, buffer.getInt(4));
        oak.remove(key);
        assertFalse(oak.computeIfPresent(key, func2));
        thrown.expect(NullPointerException.class);
        assertEquals(8, buffer.remaining());
    }

    @Test
    public void testValuesTransformIterator() {
        for (int i = 0; i < 100; i++) {
            ByteBuffer key = ByteBuffer.allocate(4);
            key.putInt(i);
            key.flip();
            ByteBuffer value = ByteBuffer.allocate(4);
            value.putInt(i);
            value.flip();
            oak.put(key, value);
        }

        Function<ByteBuffer,Integer> function = new Function<ByteBuffer, Integer>() {
            @Override
            public Integer apply(ByteBuffer byteBuffer) {
                return byteBuffer.getInt();
            }
        };
        Iterator<Integer> iter = oak.valuesTransformIterator(function);

        for (int i = 0; i < 100; i++) {
            assertEquals(i, (int) iter.next());
        }
    }

    @Test
    public void testEntriesTransformIterator() {
        for (int i = 0; i < 100; i++) {
            ByteBuffer key = ByteBuffer.allocate(4);
            key.putInt(i);
            key.flip();
            ByteBuffer value = ByteBuffer.allocate(4);
            value.putInt(i + 1);
            value.flip();
            oak.put(key, value);
        }

        Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer> function = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer>() {
            @Override
            public Integer apply(Map.Entry<ByteBuffer, ByteBuffer> entry) {
                int key = entry.getKey().getInt();
                int value = entry.getValue().getInt();
                return value - key;
            }
        };
        Iterator<Integer> iter = oak.entriesTransformIterator(function);

        for (int i = 0; i < 100; i++) {
            assertEquals(1, (int) iter.next());
        }
    }
}
