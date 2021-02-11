/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;

public final class UnsafeUtils {

    static Unsafe unsafe;

    // Used for direct access to int array
    static final long INT_ARRAY_OFFSET;

    // static constructor - access and create a new instance of Unsafe
    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
            INT_ARRAY_OFFSET = unsafe.arrayBaseOffset(int[].class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private UnsafeUtils() {
    }

    static final long LONG_INT_MASK = (1L << Integer.SIZE) - 1L;

    /**
     * Combines two integers into one long where the first argument is placed in the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static long intsToLong(int i1, int i2) {
        return (i1 & LONG_INT_MASK) | (((long) i2) << Integer.SIZE);
    }

    public static byte get(long address) {
        return unsafe.getByte(address);
    }

    public static char getChar(long address) {
        return unsafe.getChar(address);
    }

    public static short getShort(long address) {
        return unsafe.getShort(address);
    }

    public static int getInt(long address) {
        return unsafe.getInt(address);
    }

    public static long getLong(long address) {
        return unsafe.getLong(address);
    }

    public static float getFloat(long address) {
        return unsafe.getFloat(address);
    }

    public static double getDouble(long address) {
        return unsafe.getDouble(address);
    }

    public static void put(long address, byte value) {
        unsafe.putByte(address, value);
    }

    public static void putChar(long address, char value) {
        unsafe.putChar(address, value);
    }

    public static void putShort(long address, short value) {
        unsafe.putShort(address, value);
    }

    public static void putInt(long address, int value) {
        unsafe.putInt(address, value);
    }

    public static void putLong(long address, long value) {
        unsafe.putLong(address, value);
    }

    public static void putFloat(long address, float value) {
        unsafe.putFloat(address, value);
    }

    public static void putDouble(long address, double value) {
        unsafe.putDouble(address, value);
    }

    public static long copyToArray(long address, int[] array, int size) {
        long sizeBytes = ((long) size) * Integer.BYTES;
        unsafe.copyMemory(null, address, array, INT_ARRAY_OFFSET, sizeBytes);
        return sizeBytes;
    }

    public static long copyFromArray(int[] array, long address, int size) {
        long sizeBytes = ((long) size) * Integer.BYTES;
        unsafe.copyMemory(array, INT_ARRAY_OFFSET, null, address, sizeBytes);
        return sizeBytes;
    }
}
