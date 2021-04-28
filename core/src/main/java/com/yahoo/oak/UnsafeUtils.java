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

public final class UnsafeUtils {

    static final Unsafe UNSAFE;
    static final long INT_ARRAY_OFFSET;

    // static constructor - access and create a new instance of Unsafe
    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            UNSAFE = unsafeConstructor.newInstance();
            INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private UnsafeUtils() {
    }

    static long allocateMemory(long capacity) {
        return UnsafeUtils.UNSAFE.allocateMemory(capacity);
    }

    static void freeMemory(long address) {
        UnsafeUtils.UNSAFE.freeMemory(address);
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
        return UNSAFE.getByte(address);
    }

    public static char getChar(long address) {
        return UNSAFE.getChar(address);
    }

    public static short getShort(long address) {
        return UNSAFE.getShort(address);
    }

    public static int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    public static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    public static float getFloat(long address) {
        return UNSAFE.getFloat(address);
    }

    public static double getDouble(long address) {
        return UNSAFE.getDouble(address);
    }

    public static void put(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    public static void putChar(long address, char value) {
        UNSAFE.putChar(address, value);
    }

    public static void putShort(long address, short value) {
        UNSAFE.putShort(address, value);
    }

    public static void putInt(long address, int value) {
        UNSAFE.putInt(address, value);
    }

    public static void putLong(long address, long value) {
        UNSAFE.putLong(address, value);
    }

    public static void putFloat(long address, float value) {
        UNSAFE.putFloat(address, value);
    }

    public static void putDouble(long address, double value) {
        UNSAFE.putDouble(address, value);
    }

    public static long copyToArray(long address, int[] array, int size) {
        long sizeBytes = ((long) size) * Integer.BYTES;
        UNSAFE.copyMemory(null, address, array, INT_ARRAY_OFFSET, sizeBytes);
        return sizeBytes;
    }

    public static long copyFromArray(int[] array, long address, int size) {
        long sizeBytes = ((long) size) * Integer.BYTES;
        UNSAFE.copyMemory(array, INT_ARRAY_OFFSET, null, address, sizeBytes);
        return sizeBytes;
    }

    /*-------------- Wrapping address with bytebuffer --------------*/
    private static final Field ADDRESS;
    private static final Field CAPACITY;
    static {
        try {
            ADDRESS = Buffer.class.getDeclaredField("address");
            CAPACITY = Buffer.class.getDeclaredField("capacity");
            ADDRESS.setAccessible(true);
            CAPACITY.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    public static ByteBuffer wrapAddress(long memAddress, int capacity) {
        ByteBuffer bb = ByteBuffer.allocateDirect(0);
        try {
            ADDRESS.setLong(bb, memAddress);
            CAPACITY.setInt(bb, capacity);
            bb.clear();
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
        return bb;
    }
}
