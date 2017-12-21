package oak;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleThreadTest {

    private OakMapOnHeapImpl oak;

    @Before
    public void init() {
        oak = new OakMapOnHeapImpl();
    }

    private int countNumOfChunks() {
        Chunk c = oak.skiplist.firstEntry().getValue();
        int counter = 1;
        Chunk next = c.next.getReference();
        assertFalse(c.next.isMarked());
        while (next != null) {
            assertFalse(next.next.isMarked());
            counter++;
            next = next.next.getReference();
        }
        return counter;
    }

    @Test
    public void testPutAndGet() {
        assertEquals(1, countNumOfChunks());
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        assertEquals(4, countNumOfChunks());
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        assertEquals(4, countNumOfChunks());
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(10);
        bb.flip();
        OakBuffer buffer = oak.get(bb);
        assertTrue(buffer != null);
        TestCase.assertEquals(10, buffer.getInt(0));
        boolean check = false;
        try{
            buffer.getInt(4);
        } catch (Exception exp){
            check = true;
        }
        assertTrue(check);
        ByteBuffer bb2 = ByteBuffer.allocate(4);
        bb2.putInt(11);
        bb2.flip();
        oak.put(bb, bb2);
        buffer = oak.get(bb);
        assertTrue(buffer != null);
        TestCase.assertEquals(11, buffer.getInt(0));
    }

    @Test
    public void testPutIfAbsent() {
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            assertTrue(oak.putIfAbsent(bb, bb));
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
            ByteBuffer bb1 = ByteBuffer.allocate(4);
            bb1.putInt(i + 1);
            bb1.flip();
            assertFalse(oak.putIfAbsent(bb, bb1));
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
    public void testRemoveAndGet() {
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        assertEquals(1, countNumOfChunks());
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        assertEquals(1, countNumOfChunks());
    }

    @Test
    public void testGraduallyRemove() {
        for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        assertEquals(8, countNumOfChunks());
        for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        for (int i = 2 * Chunk.MAX_ITEMS; i < 3 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        for (int i = Chunk.MAX_ITEMS; i < 2 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        Assert.assertTrue(countNumOfChunks() < 8);
        int countNow = countNumOfChunks();
        for (int i = Chunk.MAX_ITEMS; i < 3 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        for (int i = 0; i < Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        for (int i = 0; i < Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        for (int i = 0; i < 3 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        for (int i = 3 * Chunk.MAX_ITEMS; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        Assert.assertTrue(countNumOfChunks() < countNow);
        for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (int i = 0; i < 4 * Chunk.MAX_ITEMS; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            TestCase.assertEquals(i, buffer.getInt(0));
        }
        assertEquals(8, countNumOfChunks());
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testComputeIf() {
        Consumer<WritableOakBuffer> func = writableOakBuffer -> {
            if (writableOakBuffer.getInt() == 0) writableOakBuffer.putInt(0, 1);
        };
        ByteBuffer key = ByteBuffer.allocate(4);
        key.putInt(0);
        key.flip();
        assertFalse(oak.computeIfPresent(key, func));
        assertTrue(oak.putIfAbsent(key, key));
        OakBuffer buffer = oak.get(key);
        assertTrue(buffer != null);
        assertEquals(0, buffer.getInt(0));
        assertTrue(oak.computeIfPresent(key, func));
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
        assertTrue(oak.computeIfPresent(key, func2));
        assertEquals(8, buffer.remaining());
        assertEquals(0, buffer.getInt(0));
        assertEquals(1, buffer.getInt(4));
        oak.remove(key);
        assertFalse(oak.computeIfPresent(key, func2));
        thrown.expect(NullPointerException.class);
        assertEquals(8, buffer.remaining());
    }

    @Test
    public void testCompute() {
        Consumer<WritableOakBuffer> func = writableOakBuffer -> {
            if (writableOakBuffer.getInt() == 0) writableOakBuffer.putInt(0, 1);
        };
        ByteBuffer key = ByteBuffer.allocate(4);
        key.putInt(0);
        key.flip();
        assertFalse(oak.computeIfPresent(key, func));
        oak.putIfAbsentComputeIfPresent(key,() -> key,func);
        OakBuffer buffer = oak.get(key);
        assertTrue(buffer != null);
        assertEquals(0, buffer.getInt(0));
        oak.putIfAbsentComputeIfPresent(key,() -> key,func);
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
        oak.putIfAbsentComputeIfPresent(key,() -> key,func2);
        assertEquals(8, buffer.remaining());
        assertEquals(0, buffer.getInt(0));
        assertEquals(1, buffer.getInt(4));
        oak.remove(key);
        assertFalse(oak.computeIfPresent(key, func2));
        thrown.expect(NullPointerException.class);
        assertEquals(8, buffer.remaining());
    }
}
