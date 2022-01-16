/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Support read/write API to direct memory address.
 * It uses {@link sun.misc.Unsafe} API (Java 8).
 * As long as Oak is compiled for Java 8, this API should work as intended even if Oak is used by projects that
 * compiled in newer Java versions.
 */
public final class DirectUtils {
    static final Unsafe UNSAFE;
    static final long INT_ARRAY_OFFSET;

    static final long LONG_INT_MASK = (1L << Integer.SIZE) - 1L;

    private static final Field ADDRESS;
    private static final Field CAPACITY;

    static {
        // Access and create a new instance of Unsafe
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            UNSAFE = unsafeConstructor.newInstance();
            INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Wrapping address with bytebuffer
        try {
            ADDRESS = Buffer.class.getDeclaredField("address");
            CAPACITY = Buffer.class.getDeclaredField("capacity");
            ADDRESS.setAccessible(true);
            CAPACITY.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private DirectUtils() {
    }

    /**
     * Allocate memory. The returned address should be freed explicitly using {@link DirectUtils#freeMemory(long)}.
     * @param capacity the memory capacity to allocate
     * @return an address to the newly allocated memory.
     */
    public static long allocateMemory(long capacity) {
        return DirectUtils.UNSAFE.allocateMemory(capacity);
    }

    /**
     * Releases memory that was allocated via {@link DirectUtils#allocateMemory(long)}.
     * @param address the memory address to release.
     */
    public static void freeMemory(long address) {
        DirectUtils.UNSAFE.freeMemory(address);
    }

    /**
     * Initialize a memory block.
     * @param address the memory address of the block
     * @param bytes the number of bytes to write
     * @param value the value to write
     */
    public static void setMemory(long address, long bytes, byte value) {
        UNSAFE.setMemory(address, bytes, value);
    }

    /**
     * Read a byte value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static byte get(long address) {
        return UNSAFE.getByte(address);
    }

    /**
     * Read a char value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static char getChar(long address) {
        return UNSAFE.getChar(address);
    }

    /**
     * Read a short value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static short getShort(long address) {
        return UNSAFE.getShort(address);
    }

    /**
     * Read an int value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    /**
     * Read a long value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    /**
     * Read a float value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static float getFloat(long address) {
        return UNSAFE.getFloat(address);
    }

    /**
     * Read a double value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static double getDouble(long address) {
        return UNSAFE.getDouble(address);
    }

    /**
     * Puts a byte value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void put(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    /**
     * Puts a char value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putChar(long address, char value) {
        UNSAFE.putChar(address, value);
    }

    /**
     * Puts a short value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putShort(long address, short value) {
        UNSAFE.putShort(address, value);
    }

    /**
     * Puts an int value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putInt(long address, int value) {
        UNSAFE.putInt(address, value);
    }

    /**
     * Puts a long value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putLong(long address, long value) {
        UNSAFE.putLong(address, value);
    }

    /**
     * Puts a float value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putFloat(long address, float value) {
        UNSAFE.putFloat(address, value);
    }

    /**
     * Puts a double value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putDouble(long address, double value) {
        UNSAFE.putDouble(address, value);
    }

    /**
     * Copy data from a direct memory address to a int array.
     * @param address the source memory address
     * @param array the target array
     * @param size the number of integers to copy
     * @return the number of bytes that were copied
     */
    public static long copyToArray(long address, int[] array, int size) {
        long sizeBytes = ((long) size) * Integer.BYTES;
        UNSAFE.copyMemory(null, address, array, INT_ARRAY_OFFSET, sizeBytes);
        return sizeBytes;
    }

    /**
     * Copies int array to a direct memory block byte wise.
     * @param array the source int array
     * @param address the target memory address
     * @param size the number of integers to copy
     * @return the number of bytes that were copied
     */
    public static long copyFromArray(int[] array, long address, int size) {
        long sizeBytes = ((long) size) * Integer.BYTES;
        UNSAFE.copyMemory(array, INT_ARRAY_OFFSET, null, address, sizeBytes);
        return sizeBytes;
    }

    /**
     * Wraps a memory address as a direct byte buffer.
     * @param address the source memory address to wrap
     * @param capacity the new byte buffer capacity
     * @return a direct byte buffer instance
     */
    public static ByteBuffer wrapAddress(long address, int capacity) {
        ByteBuffer bb = ByteBuffer.allocateDirect(0);
        try {
            ADDRESS.setLong(bb, address);
            CAPACITY.setInt(bb, capacity);
            bb.clear();
        } catch (IllegalAccessException e) {
            // This should never happen because the fields are set as accessible.
            throw new RuntimeException(e);
        }
        return bb;
    }

    /**
     * Combines two integers into one long where the first argument is placed in the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     * @param i1 the number stored in the lower part of the output
     * @param i2 the number stored in the upper part of the output
     * @return combination of the two integers as long
     */
    public static long intsToLong(int i1, int i2) {
        return (i1 & LONG_INT_MASK) | (((long) i2) << Integer.SIZE);
    }
}
