package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

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
        oak.ZC().put(0, initialValue);
        assertEquals("ZC insertion of new key should increase size by 1", 1, oak.size());

        oak.ZC().put(0, initialValue + 1);
        assertEquals("ZC insertion of existing key should not increase size", 1, oak.size());
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

        OakRBuffer result = oak.ZC().get(key);
        assertNotNull("Looking up an existing key should return non-null OakRBuffer", result);
        int actualValue = result.getInt(0);
        assertEquals("Looking up an existing key should return the mapped value", expectedValue, actualValue);
        assertNull("Looking up a non-existing key should return null", oak.ZC().get(key + 1));
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

        /* .ZC().remove(K) */
        oak.put(key, expectedValue);
        oak.ZC().remove(key);
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

    @Test
    public void replace() {
        int key = r.nextInt(), val1 = r.nextInt(), val2 = r.nextInt();
        oak.put(key, val1);

        /* Replace(K, V) */
        assertNull("Replacing non-existing key should return null", oak.replace(key + 1, val1));
        assertEquals("Replacing existing key should return previous value", val1, (int) oak.replace(key, val2));
        assertEquals("Replacing existing key should replace the value", val2, (int) oak.get(key));

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
        int numKyes = 10;
        List<Integer> keys = new ArrayList<>(numKyes);
        for (int i = 0; i < numKyes; i++) {
            keys.add(i);
            oak.put(i, i);
        }
        NavigableSet<Integer> keySet = oak.keySet();

        assertEquals(numKyes, keySet.size());
        for (int i = 0; i < numKyes; i++) {
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
        int numKyes = 10;
        List<Integer> keys = new ArrayList<>(numKyes);
        for (int i = 0; i < numKyes; i++) {
            keys.add(i);
            oak.put(i, i);
        }

        Set<Map.Entry<Integer, Integer>> entries = oak.entrySet();
        assertEquals(numKyes, entries.size());
        for (int i = 0; i < numKyes; i++) {
            assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>(i, i)));
        }

        entries = oak.subMap(3, 5).entrySet();
        for (int i = 3; i < 5; i++) {
            assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>(i, i)));
        }

        assertFalse(entries.contains(new AbstractMap.SimpleImmutableEntry<>(0, 0)));
        assertFalse(entries.contains(new AbstractMap.SimpleImmutableEntry<>(8, 8)));

        entries.forEach(e -> {
            assertEquals(e.getKey(), e.getValue());
        });
    }

    @Test
    public void putIfAbsent() {
        assertNull("putIfAbsent should return null if mapping doesn't exist", oak.putIfAbsent(0, 0));
        assertEquals("putIfAbsent should insert an item if mapping doesn't exist", 1, oak.size());
        assertEquals("putIfAbsent should return previous value if mapping exists", 0, (int) oak.putIfAbsent(0, 1));
        assertEquals("putIfAbsent should not insert an item if mapping doesn't exist", 1, oak.size());
    }

    @Test
    public void putIfAbsentZC() {
        assertTrue("putIfAbsentZC should return true if mapping doesn't exist", oak.ZC().putIfAbsent(0, 0));
        assertEquals("putIfAbsent should insert an item if mapping doesn't exist", 1, oak.size());
        assertFalse("putIfAbsent should return previous value if mapping exists", oak.ZC().putIfAbsent(0, 1));
        assertEquals("putIfAbsent should not insert an item if mapping doesn't exist", 1, oak.size());
    }
}