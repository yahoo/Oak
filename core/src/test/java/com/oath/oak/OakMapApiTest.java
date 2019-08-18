package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ReadOnlyBufferException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class OakMapApiTest {

    private OakMap<Integer, Integer> oak;
    private Random r = new Random();


    @Before
    public void init() {
        int maxItemsPerChunk = 2048;
        int maxBytesPerChunkItem = Integer.BYTES;
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder()
                .setChunkMaxItems(maxItemsPerChunk)
                .setChunkBytesPerItem(maxBytesPerChunkItem);
        oak = builder.build();
    }

    @After
    public void finish() {
        oak.close();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void size() {
        assertEquals("Initial size should be 0", 0, oak.size());
        oak.put(0, 0);
        assertEquals("Insertion of new key should increase size by 1", 1, oak.size());
        oak.put(0, 1);
        assertEquals("Insertion of existing key should not increase size", 1, oak.size());

        oak.remove(0);
        assertEquals("Removal of existing key should decrease size by 1", 0, oak.size());

        // Should throw UnsupportedOperationException
        oak.subMap(0, 1).size();
    }

    @Test
    public void isEmpty() {
        assertTrue("New OakMap should be empty", oak.isEmpty());
        oak.put(0, 0);
        assertFalse("Insertion of new key should make OakMap non-empty", oak.isEmpty());
        oak.remove(0);
        assertTrue("Removal of all keys should make OakMap empty", oak.isEmpty());
    }


    @Test
    public void put() {
        int initialValue = r.nextInt();
        Integer res = oak.put(0, initialValue);
        assertNull("Inserting a new key should return null", res);

        res = oak.put(0, initialValue + 1);
        assertNotNull("Inserting an existing key should return a value", res);
        assertEquals("Inserting an existing key should return old value", initialValue, (int) res);
    }

    @Test
    public void putZC() {
        int initialValue = r.nextInt();
        oak.zc().put(0, initialValue);
        assertEquals("zc insertion of new key should increase size by 1", 1, oak.size());

        oak.zc().put(0, initialValue + 1);
        assertEquals("zc insertion of existing key should not increase size", 1, oak.size());
    }


    @Test
    public void get() {
        int key = r.nextInt(), expectedValue = r.nextInt();
        oak.put(key, expectedValue);

        assertEquals("Looking up an existing key should return the mapped value", expectedValue, (int) oak.get(key));
        assertNull("Looking up a non-existing key should return null", oak.get(key + 1));
    }

    @Test
    public void getZC() {
        int key = r.nextInt(), expectedValue = r.nextInt();
        oak.put(key, expectedValue);

        OakRBuffer result = oak.zc().get(key);
        assertNotNull("Looking up an existing key should return non-null OakRBuffer", result);
        int actualValue = result.getInt(0);
        assertEquals("Looking up an existing key should return the mapped value", expectedValue, actualValue);
        assertNull("Looking up a non-existing key should return null", oak.zc().get(key + 1));
    }

    @Test
    public void remove() {
        int key = r.nextInt(), expectedValue = r.nextInt();

        /* Remove(K) */
        oak.put(key, expectedValue);
        Integer removed = oak.remove(key);
        assertNotNull("Removing an existing key should return a value", removed);
        assertEquals("Remove should return old value", expectedValue, (int) removed);
        assertNull("Remove should remove the mapping from the map", oak.get(key));
        assertNull("Removing an non-existing key should return null", oak.remove(key));

        /* Remove(K, V) */
        oak.put(key, expectedValue);
        assertFalse("Removing a key with non-matching value should return false", oak.remove(key, expectedValue + 1));
        assertTrue("Removing a key with matching value should return true", oak.remove(key, expectedValue));
        assertNull("Removing a key with non-matching value should not remove the value", oak.remove(key));
    }

    @Test
    public void removeZC() {
        int key = r.nextInt(), expectedValue = r.nextInt();

        /* zc().remove(K) */
        oak.put(key, expectedValue);
        oak.zc().remove(key);
        assertNull("Remove should remove the mapping from the map", oak.get(key));
    }

    @Test
    public void firstKey() {
        assertNull("Empty map minimal key should be null", oak.firstKey());

        int key1 = r.nextInt(), key2 = r.nextInt();
        int minKey = Math.min(key1, key2);
        oak.put(key1, key1);
        oak.put(key2, key2);
        assertEquals(String.format("Min key should be %d", minKey), minKey, (int) oak.firstKey());
    }

    @Test
    public void lastKey() {
        assertNull("Empty map maximal key should be null", oak.lastKey());
        int maxKey = 0;
        oak.put(maxKey, 10);
        assertEquals(String.format("Max key should be %d", maxKey), maxKey, (int) oak.lastKey());

        maxKey++;
        oak.put(maxKey, 1);
        assertEquals(String.format("Max key should be %d", maxKey), maxKey, (int) oak.lastKey());
    }

    @Ignore
    @Test
    public void replace() {
        int key = r.nextInt(), val1 = r.nextInt(), val2 = r.nextInt();
        oak.put(key, val1);

        /* Replace(K, V) */
        assertNull("Replacing non-existing key should return null", oak.replace(key + 1, val1));
        Integer result = oak.replace(key, val2);
        assertNotNull("Replacing existing key should return a non-null value", result);
        assertEquals("Replacing existing key should return previous value", val1, result.intValue());
        assertEquals("Replacing existing key should replace the value", val2, oak.get(key).intValue());

        /* Replace(K, V, V) */
        assertFalse("Replacing non-matching value should return false", oak.replace(key, val1, val2));
        assertTrue("Replacing non-matching value should return true", oak.replace(key, val2, val1));
        assertEquals("Replacing existing key should replace the value", val1, (int) oak.get(key));
    }

    @Test
    public void lowerKey() {
        oak.put(0, 0);
        oak.put(1, 1);
        oak.put(2, 2);

        assertEquals(2, (int) oak.lowerKey(3));
        assertEquals(1, (int) oak.lowerKey(2));
        assertEquals(0, (int) oak.lowerKey(1));

        assertNull(oak.lowerKey(0));
        assertNull(oak.lowerKey(Integer.MIN_VALUE));
    }

    @Test
    public void keySet() {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }
        NavigableSet<Integer> keySet = oak.keySet();

        assertEquals(numKeys, keySet.size());
        for (int i = 0; i < numKeys; i++) {
            assertTrue(keySet.contains(i));
        }

        keySet = oak.subMap(3, 5).keySet();
        for (int i = 3; i < 5; i++) {
            assertTrue(keySet.contains(i));
        }
        assertFalse(keySet.contains(0));
        assertFalse(keySet.contains(8));
    }

    @Test
    public void entrySet() {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }

        Set<Map.Entry<Integer, Integer>> entries = oak.entrySet();
        assertEquals(numKeys, entries.size());
        for (int i = 0; i < numKeys; i++) {
            assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>(i, i)));
        }

        entries = oak.subMap(3, 5).entrySet();
        for (int i = 3; i < 5; i++) {
            assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>(i, i)));
        }

        assertFalse(entries.contains(new AbstractMap.SimpleImmutableEntry<>(0, 0)));
        assertFalse(entries.contains(new AbstractMap.SimpleImmutableEntry<>(8, 8)));

        entries.forEach(e -> assertEquals(e.getKey(), e.getValue()));
    }

    @Test
    public void putIfAbsent() {
        assertNull("putIfAbsent should return null if mapping doesn't exist", oak.putIfAbsent(0, 0));
        assertEquals("putIfAbsent should insert an item if mapping doesn't exist", 1, oak.size());
        Integer result = oak.putIfAbsent(0, 1);
        assertNotNull("putIfAbsent should return a non-null value if mapping exists", result);
        assertEquals("putIfAbsent should return previous value if mapping exists", 0, result.intValue());
        assertEquals("putIfAbsent should not insert an item if mapping doesn't exist", 1, oak.size());
    }

    @Test
    public void putIfAbsentZC() {
        assertTrue("putIfAbsentZC should return true if mapping doesn't exist", oak.zc().putIfAbsent(0, 0));
        assertEquals("putIfAbsent should insert an item if mapping doesn't exist", 1, oak.size());
        assertFalse("putIfAbsent should return previous value if mapping exists", oak.zc().putIfAbsent(0, 1));
        assertEquals("putIfAbsent should not insert an item if mapping doesn't exist", 1, oak.size());
    }

    @Ignore
    @Test
    public void computeIfPresent() {
        BiFunction<? super Integer, ? super Integer, ? extends Integer> func = (k, v) -> v * 2;

        assertNull("computeIfPresent should return null if mapping doesn't exist", oak.computeIfPresent(0, func));
        oak.put(0, 1);
        Integer result = oak.computeIfPresent(0, func);
        assertNotNull("computeIfPresent should return a non-null value if mapping exists", result);
        assertEquals("computeIfPresent should return the new value if mapping exists", 2, result.intValue());

        result = oak.get(0);
        assertNotNull("computeIfPresent should not remove an existing mapping", result);
        assertEquals("computeIfPresent should modify the existing mapping", 2, result.intValue());
    }

    @Test
    public void computeIfPresentZC() {
        Consumer<OakWBuffer> func = oakWBuffer -> oakWBuffer.putInt(0, oakWBuffer.getInt(0) * 2);

        assertFalse("computeIfPresentZC should return false if mapping doesn't exist", oak.zc().computeIfPresent(0, func));
        oak.put(0, 1);
        assertTrue("computeIfPresent should return a non-null value if mapping exists", oak.zc().computeIfPresent(0, func));
        Integer result = oak.get(0);
        assertNotNull("computeIfPresent should not remove an existing mapping", result);
        assertEquals("computeIfPresent should modify the existing mapping", 2, result.intValue());
    }

    @Test
    public void iterTest() {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            oak.put(i, i);
        }

        Integer from = 4;
        Integer to = 6;

        Integer expected = from + 1;
        try (OakMap<Integer, Integer> sub = oak.subMap(from, false, to, true)) {
            for (Integer i : sub.values()) {
                assertEquals(expected.intValue(), i.intValue());
                expected++;
            }
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void immutableKeyBuffers() {
        oak.put(0, 0);

        OakRBuffer buffer = oak.zc().keySet().iterator().next();

        buffer.transform(b -> b.putInt(0, 1));

        fail("Key Buffer should be read only");
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void immutableValueBuffers() {
        oak.put(0, 0);

        OakRBuffer buffer = oak.zc().values().iterator().next();

        buffer.transform(b -> b.putInt(0, 1));

        fail("Value Buffer should be read only");
    }
}