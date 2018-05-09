package oak;

import javafx.util.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class MemoryManagerTest {

    private SynchrobenchMemoryPoolImpl pool;
    private OakMemoryManager memoryManager;


    @Before
    public void init() {
        pool = new SynchrobenchMemoryPoolImpl(100);
        memoryManager = new OakMemoryManager(pool);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkCapacity() {

        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(4, pool.allocated());

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb1 = pair.getValue();
        assertEquals(4, bb1.remaining());
        assertEquals(8, pool.allocated());

        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb2 = pair.getValue();
        assertEquals(8, bb2.remaining());
        assertEquals(16, pool.allocated());

        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb3 = pair.getValue();
        assertEquals(8, bb3.remaining());
        assertEquals(24, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb4 = pair.getValue();
        assertEquals(8, bb4.remaining());
        assertEquals(32, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb5 = pair.getValue();
        assertEquals(8, bb5.remaining());
        assertEquals(40, pool.allocated());

        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb6 = pair.getValue();
        assertEquals(8, bb6.remaining());
        assertEquals(48, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb7 = pair.getValue();
        assertEquals(8, bb7.remaining());
        assertEquals(56, pool.allocated());
        pair = memoryManager.allocate(8);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb8 = pair.getValue();
        assertEquals(8, bb8.remaining());
        assertEquals(64, pool.allocated());

        pair = memoryManager.allocate(36);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb9 = pair.getValue();
        assertEquals(36, bb9.remaining());
        assertEquals(100, pool.allocated());

        thrown.expect(OakOutOfMemoryException.class);
        pair = memoryManager.allocate(1);
        assertEquals(0, (int) pair.getKey());
    }

    @Test
    public void checkOakCapacity() {
        OakMapOffHeapImpl oak = new OakMapOffHeapImpl(new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE));
        MemoryPool pool = oak.memoryManager.pool;


        assertEquals(Chunk.MAX_ITEMS*100,pool.allocated());
        ByteBuffer val = ByteBuffer.allocate(Integer.MAX_VALUE/20);
        byte arr[] = new byte[Integer.MAX_VALUE/20];
        arr[0] = 1;
        val.put(arr,0,arr.length);
        assertEquals(0,val.remaining());
        val.rewind();
        ByteBuffer key = ByteBuffer.allocate(4);
        key.putInt(0,0);
        assertEquals(Chunk.MAX_ITEMS*100,pool.allocated());
        oak.put(key, val);
        key.putInt(0,1);
        oak.put(key, val);
        key.putInt(0,2);
        oak.put(key, val);
        key.putInt(0,3);
        oak.put(key, val);


        key.putInt(0,0);
        OakBuffer buffer = oak.get(key);
        assertTrue(buffer != null);
        assertEquals(1, buffer.get(0));
        key.putInt(0,3);
        buffer = oak.get(key);
        assertTrue(buffer != null);
        assertEquals(1, buffer.get(0));

        for(int i = 0; i < 10; i++){
            key.putInt(0,i);
            oak.put(key, val);
            oak.remove(key);
        }
    }

    @Test
    public void checkRelease() {

        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(4, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(8, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(12, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(16, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(20, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(24, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(28, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(32, pool.allocated());
        memoryManager.release(0, bb);

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(36, pool.allocated());
        memoryManager.release(0, bb);

        assertEquals(9, memoryManager.releasedArray.get(1).size());

        pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        bb = pair.getValue();
        assertEquals(4, bb.remaining());
        assertEquals(40, pool.allocated());
        memoryManager.release(0, bb);

        if(OakMemoryManager.RELEASES == 10)
            assertEquals(0, memoryManager.releasedArray.get(1).size());
    }

    @Test
    public void checkSingleThreadRelease() {

        assertEquals(0, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.startThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.startThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(4);
        assertEquals(0, (int) pair.getKey());
        ByteBuffer bb = pair.getValue();
        bb.putInt(0,1);
        assertEquals(4, bb.remaining());
        assertEquals(4, pool.allocated());
        memoryManager.release(0, bb);
        memoryManager.stopThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.stopThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        memoryManager.startThread();
        memoryManager.startThread();
        memoryManager.startThread();
        memoryManager.stopThread();
        memoryManager.stopThread();
        memoryManager.stopThread();

        memoryManager.startThread();
        memoryManager.release(0, ByteBuffer.allocateDirect(4));
        memoryManager.stopThread();

        for(int i = 3 ; i < OakMemoryManager.RELEASES; i++){
            memoryManager.release(0, ByteBuffer.allocateDirect(4));
        }
        assertEquals(OakMemoryManager.RELEASES-1, memoryManager.releasedArray.get(1).size());
        memoryManager.startThread();
        memoryManager.release(0, ByteBuffer.allocateDirect(4));
        memoryManager.stopThread();
        assertEquals(false, memoryManager.releasedArray.get(1).isEmpty());

    }

    @Test
    public void checkStartStopThread() {

        Comparator<ByteBuffer> comparator = new IntComparator();
        ByteBuffer min = ByteBuffer.allocate(10);
        min.putInt(Integer.MIN_VALUE);
        min.flip();
        OakMapOffHeapImpl oak = new OakMapOffHeapImpl(comparator, min, new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE));
        OakMemoryManager memoryManager = oak.memoryManager;

        assertEquals(0, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.startThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.stopThread();
        assertEquals(1, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        ByteBuffer key = ByteBuffer.allocate(4);
        key.putInt(0);
        key.flip();
        oak.put(key, key);
        assertEquals(2, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0);
        bb.flip();
        OakBuffer buffer = oak.get(bb);
        assertEquals(3, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        assertTrue(buffer != null);
        assertEquals(0, buffer.getInt(0));
        assertEquals(3, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        memoryManager.startThread();
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.stopThread();
        assertEquals(4, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));


        memoryManager.startThread();
        assertEquals(5, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.startThread();
        assertEquals(5, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        oak.get(bb);
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        assertEquals(5, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        memoryManager.stopThread();
        memoryManager.stopThread();
        assertEquals(5, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));


        oak.put(key, key);
        memoryManager.startThread();
        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.startThread();
        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        oak.put(key, key);
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        memoryManager.stopThread();
        memoryManager.stopThread();
        assertEquals(7, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));


        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        assertEquals(7 + 2 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        memoryManager.startThread();
        assertEquals(8 + 2 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        assertEquals(8 + 2 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        memoryManager.stopThread();
        assertEquals(8 + 2 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));

        for (int i = 0; i < 2 * Chunk.MAX_ITEMS; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }

        try (CloseableIterator iter = oak.entriesIterator()) {
            assertEquals(9 + 4 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            int i = 0;
            while (iter.hasNext()) {
                Map.Entry<ByteBuffer, OakBuffer> e = (Map.Entry<ByteBuffer, OakBuffer>) iter.next();
                assertEquals(i, ((OakBuffer) (e.getValue())).getInt(0));
                assertEquals(i, e.getKey().getInt(0));
                i++;
            }
            assertEquals(2 * Chunk.MAX_ITEMS, i);
        }

        try (CloseableIterator iter = oak.valuesIterator()) {
            assertEquals(10 + 4 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            int i = 0;
            while (iter.hasNext()) {
                assertEquals(i, ((OakBuffer) (iter.next())).getInt(0));
                i++;
            }
            oak.get(bb);
            assertEquals(2 * Chunk.MAX_ITEMS, i);
            assertEquals(10 + 4 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            try (CloseableIterator<OakBuffer> ignored = oak.descendingMap().valuesIterator()) {
                assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
            }
            assertFalse(memoryManager.isIdle(memoryManager.timeStamps[1].get()));
        }
        assertEquals(10 + 4 * Chunk.MAX_ITEMS, memoryManager.getValue(memoryManager.timeStamps[1].get()));
        assertTrue(memoryManager.isIdle(memoryManager.timeStamps[1].get()));


    }

    @Test
    public void checkOneChunk() {

        Comparator<ByteBuffer> comparator = new IntComparator();
        ByteBuffer min = ByteBuffer.allocate(10);
        min.putInt(Integer.MIN_VALUE);
        min.flip();
        OakMapOffHeapImpl oak = new OakMapOffHeapImpl(comparator, min, new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE));

        ByteBuffer bb;
        OakBuffer buffer;

        bb = ByteBuffer.allocateDirect(4);
        bb.putInt(128);
        bb.flip();
        oak.put(bb, bb);

        for (int i = 0; i < 270; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }

        for (int i = 0; i < 270; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }

    }

    @Test
    public void releaseCalls() {
        Comparator<ByteBuffer> comparator = new IntComparator();
        ByteBuffer min = ByteBuffer.allocate(10);
        min.putInt(Integer.MIN_VALUE);
        min.flip();
        OakMapOffHeapImpl oak = new OakMapOffHeapImpl(comparator, min, new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE));
        OakMemoryManager memoryManager = oak.memoryManager;

        ByteBuffer bb;

        assertEquals(0, memoryManager.releasedArray.get(1).size());

        for (int i = 0; i < 280; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }

        assertEquals(1, memoryManager.releasedArray.get(1).size());

        bb = ByteBuffer.allocateDirect(4);
        bb.putInt(0);
        bb.flip();
        oak.remove(bb);

        assertEquals(2, memoryManager.releasedArray.get(1).size());

        oak.put(bb, bb);
        assertEquals(2, memoryManager.releasedArray.get(1).size());

        for (int i = 0; i < 100; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
            if(OakMemoryManager.RELEASES == 10) {
                if (i < 7) {
                    assertEquals(3 + i, memoryManager.releasedArray.get(1).size());
                } else { // TODO fix test
                    assertEquals((3 + i) % 10, memoryManager.releasedArray.get(1).size());
                }
            } else if (OakMemoryManager.RELEASES > 100){
                assertEquals(3 + i, memoryManager.releasedArray.get(1).size());
            }
        }

        int now = memoryManager.releasedArray.get(1).size();

        for (int i = 0; i < 280; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }

        assertEquals(now, memoryManager.releasedArray.get(1).size());

        for (int i = 0; i < 280; i++) {
            bb = ByteBuffer.allocateDirect(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals(i, buffer.getInt(0));
        }

        assertEquals(now, memoryManager.releasedArray.get(1).size());

    }

    @Test
    public void compute(){
        Consumer<WritableOakBuffer> func = buffer -> {
            if (buffer.getInt(0) == 0) {
                buffer.putInt(0, 1);
            }
        };
        OakMapOffHeapImpl oak = new OakMapOffHeapImpl(new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE));
        ByteBuffer bb = ByteBuffer.allocateDirect(4);
        bb.putInt(0,0);
        oak.put(bb,bb);
        OakBuffer buffer = oak.get(bb);
        assertTrue(buffer != null);
        assertEquals(0, buffer.getInt(0));
        oak.computeIfPresent(bb, func);
        buffer = oak.get(bb);
        assertTrue(buffer != null);
        assertEquals(1, buffer.getInt(0));
        oak.computeIfPresent(bb, func);
        buffer = oak.get(bb);
        assertTrue(buffer != null);
        assertEquals(1, buffer.getInt(0));
        func = buf -> {
            if (buf.getInt(0) == 1) {
                buf.putInt(1);
                buf.putInt(1);
            }
        };
        oak.computeIfPresent(bb, func);
        buffer = oak.get(bb);
        assertTrue(buffer != null);
        assertEquals(1, buffer.getInt(0));
        assertEquals(1, buffer.getInt(4));



        oak.close();

    }


}
