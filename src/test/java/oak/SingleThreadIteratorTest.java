package oak;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleThreadIteratorTest {

    private OakMapOnHeapImpl oak;
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
        oak = new OakMapOnHeapImpl(comparator, min, maxItemsPerChunk, maxBytesPerChunkItem);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testIterator() {
        Integer i;
        for (i = 0; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (i = 0; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals((int) i, buffer.getInt(0));
        }
        Iterator valIter = oak.valuesIterator();
        Iterator entryIter = oak.entriesIterator();
        i = 0;
        while (valIter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (valIter.next())).getInt(0));
            Map.Entry<ByteBuffer, OakBuffer> e = (Map.Entry<ByteBuffer, OakBuffer>) entryIter.next();
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            assertEquals(bb, e.getKey());
            assertEquals((int) i, e.getValue().getInt(0));
            i++;
        }
        for (i = 0; i < maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        for (i = 0; i < maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals((int) i, buffer.getInt(0));
        }
        valIter = oak.valuesIterator();
        entryIter = oak.entriesIterator();
        i = maxItemsPerChunk;
        while (valIter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (valIter.next())).getInt(0));
            Map.Entry<ByteBuffer, OakBuffer> e = (Map.Entry<ByteBuffer, OakBuffer>) entryIter.next();
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            assertEquals(bb, e.getKey());
            assertEquals((int) i, e.getValue().getInt(0));
            i++;
        }
        for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }
        for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer == null);
        }
        for (i = 0; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (i = 1; i < (2 * maxItemsPerChunk - 1); i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.remove(bb);
        }

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0);
        bb.flip();
        OakBuffer buffer = oak.get(bb);
        assertTrue(buffer != null);
        assertEquals(0, buffer.getInt(0));

        bb = ByteBuffer.allocate(4);
        bb.putInt((2 * maxItemsPerChunk - 1));
        bb.flip();
        buffer = oak.get(bb);
        assertTrue(buffer != null);
        assertEquals((2 * maxItemsPerChunk - 1), buffer.getInt(0));

        valIter = oak.valuesIterator();
        assertTrue(valIter.hasNext());
        assertEquals(0, ((OakBuffer) (valIter.next())).getInt(0));
        assertTrue(valIter.hasNext());
        assertEquals((2 * maxItemsPerChunk - 1), ((OakBuffer) (valIter.next())).getInt(0));
        thrown.expect(java.util.NoSuchElementException.class);
        ((OakBuffer) (valIter.next())).getInt(0);
    }

    @Test
    public void testGetRange() {
        ByteBuffer from = ByteBuffer.allocate(4);
        from.putInt(0);
        from.flip();
        ByteBuffer to = ByteBuffer.allocate(4);
        to.putInt(3 * maxItemsPerChunk);
        to.flip();

        OakMap sub = oak.subMap(from, true, to, false);
        Iterator iter = sub.valuesIterator();
        assertFalse(iter.hasNext());

        for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (int i = 0; i < 12 * maxItemsPerChunk; i++) {
            if (i % 3 == 0) {
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(i);
                bb.flip();
                oak.remove(bb);
            }
        }

        iter = sub.valuesIterator();
        int c = 0;
        while (iter.hasNext()) {
            if (c % 3 == 0) {
                c++;
            }
            assertEquals(c, ((OakBuffer) (iter.next())).getInt(0));
            c++;
        }
        assertEquals(3 * maxItemsPerChunk, c);

        from = ByteBuffer.allocate(4);
        from.putInt(6 * maxItemsPerChunk);
        from.flip();
        to = ByteBuffer.allocate(4);
        to.putInt(9 * maxItemsPerChunk);
        to.flip();
        sub = oak.subMap(from, true, to, false);
        iter = sub.valuesIterator();
        c = 6 * maxItemsPerChunk;
        while (iter.hasNext()) {
            if (c % 3 == 0) {
                c++;
            }
            assertEquals(c, ((OakBuffer) (iter.next())).getInt(0));
            c++;
        }
        assertEquals(9 * maxItemsPerChunk, c);

        from = ByteBuffer.allocate(4);
        from.putInt(9 * maxItemsPerChunk);
        from.flip();
        to = ByteBuffer.allocate(4);
        to.putInt(13 * maxItemsPerChunk);
        to.flip();
        sub = oak.subMap(from, true, to, false);
        iter = sub.valuesIterator();
        c = 9 * maxItemsPerChunk;
        while (iter.hasNext()) {
            if (c % 3 == 0) {
                c++;
            }
            assertEquals(c, ((OakBuffer) (iter.next())).getInt(0));
            c++;
        }
        assertEquals(12 * maxItemsPerChunk, c);

        from = ByteBuffer.allocate(4);
        from.putInt(12 * maxItemsPerChunk);
        from.flip();
        to = ByteBuffer.allocate(4);
        to.putInt(13 * maxItemsPerChunk);
        to.flip();
        sub = oak.subMap(from, true, to, false);
        iter = sub.valuesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testEntryKeySet() {
        Integer i;
        for (i = 0; i < 5; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (i = 0; i < 5; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals((int) i, buffer.getInt(0));
        }
        Iterator iter = oak.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals(0, i.intValue());
        OakMap map = oak.descendingMap();
        iter = map.descendingMap().valuesIterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
            i++;
        }
        assertEquals(5, i.intValue());
        iter = map.entriesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            Map.Entry<ByteBuffer, OakBuffer> e = (Map.Entry<ByteBuffer, OakBuffer>) iter.next();
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            assertEquals(bb, e.getKey());
            assertEquals((int) i, e.getValue().getInt(0));
        }
        assertEquals(0, i.intValue());

        ByteBuffer from = ByteBuffer.allocate(4);
        from.putInt(1);
        from.flip();
        ByteBuffer to = ByteBuffer.allocate(4);
        to.putInt(4);
        to.flip();

        OakMap sub = oak.subMap(from, false, to, true);
        iter = sub.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals(2, i.intValue());
    }

    @Test
    public void testDescending() {
        Iterator iter = oak.descendingMap().valuesIterator();
        assertFalse(iter.hasNext());

        Integer i;
        for (i = 0; i < 5; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (i = 0; i < 5; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals((int) i, buffer.getInt(0));
        }

        iter = oak.valuesIterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
            i++;
        }
        assertEquals(5, i.intValue());

        iter = oak.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals(0, i.intValue());

        ByteBuffer from = ByteBuffer.allocate(4);
        from.putInt(1);
        from.flip();
        ByteBuffer to = ByteBuffer.allocate(4);
        to.putInt(4);
        to.flip();

        OakMap sub = oak.subMap(from, false, to, true);
        iter = sub.valuesIterator();
        i = 2;
        while (iter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
            i++;
        }
        assertEquals(5, i.intValue());

        iter = sub.descendingMap().valuesIterator();
        i = 5;
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals(2, i.intValue());


        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            oak.put(bb, bb);
        }
        for (i = 0; i < 3 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            OakBuffer buffer = oak.get(bb);
            assertTrue(buffer != null);
            assertEquals((int) i, buffer.getInt(0));
        }

        iter = oak.valuesIterator();
        i = 0;
        while (iter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
            i++;
        }
        assertEquals(3 * maxItemsPerChunk, i.intValue());

        iter = oak.descendingMap().valuesIterator();
        i = 3 * maxItemsPerChunk;
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals(0, i.intValue());

        from = ByteBuffer.allocate(4);
        from.putInt(2 * maxItemsPerChunk);
        from.flip();
        to = ByteBuffer.allocate(4);
        to.putInt(3 * maxItemsPerChunk);
        to.flip();

        sub = oak.subMap(from, true, to, false);
        iter = sub.valuesIterator();
        i = 2 * maxItemsPerChunk;
        while (iter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
            i++;
        }
        assertEquals(3 * maxItemsPerChunk, i.intValue());
        iter = sub.descendingMap().valuesIterator();
        i = 3 * maxItemsPerChunk;
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals(2 * maxItemsPerChunk, i.intValue());

        from = ByteBuffer.allocate(4);
        from.putInt((int) Math.round(0.1 * maxItemsPerChunk));
        from.flip();
        to = ByteBuffer.allocate(4);
        to.putInt((int) Math.round(2.3 * maxItemsPerChunk));
        to.flip();

        sub = oak.subMap(from, true, to, false);
        iter = sub.valuesIterator();
        i = (int) Math.round(0.1 * maxItemsPerChunk);
        while (iter.hasNext()) {
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
            i++;
        }
        assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());
        iter = sub.descendingMap().valuesIterator();
        i = (int) Math.round(2.3 * maxItemsPerChunk);
        while (iter.hasNext()) {
            i--;
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals((int) Math.round(0.1 * maxItemsPerChunk), i.intValue());

        for (i = maxItemsPerChunk; i < 2 * maxItemsPerChunk; i++) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            bb.flip();
            sub.remove(bb);
        }
        iter = sub.valuesIterator();
        i = (int) Math.round(0.1 * maxItemsPerChunk);
        while (iter.hasNext()) {
            if (i == maxItemsPerChunk) {
                i = 2 * maxItemsPerChunk;
            }
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
            i++;
        }
        assertEquals((int) Math.round(2.3 * maxItemsPerChunk), i.intValue());
        iter = sub.descendingMap().valuesIterator();
        i = (int) Math.round(2.3 * maxItemsPerChunk);
        while (iter.hasNext()) {
            i--;
            if (i == 2 * maxItemsPerChunk - 1) {
                i = maxItemsPerChunk - 1;
            }
            assertEquals((int) i, ((OakBuffer) (iter.next())).getInt(0));
        }
        assertEquals((int) Math.round(0.1 * maxItemsPerChunk), i.intValue());
    }

}
