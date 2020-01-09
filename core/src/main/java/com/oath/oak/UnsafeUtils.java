package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

public final class UnsafeUtils {


    static Unsafe unsafe;

    static private final long INT_ARRAY_OFFSET;
    static private final long BYTE_ARRAY_OFFSET;

    // static constructor - access and create a new instance of Unsafe
    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        INT_ARRAY_OFFSET = unsafe.arrayBaseOffset(int[].class);
        BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    }

    private UnsafeUtils() {
    }

    public static void unsafeCopyBufferToIntArray(ByteBuffer srcByteBuffer, int position, int[] dstArray, int countInts) {
        if (srcByteBuffer.isDirect()) {
            long bbAddress = ((DirectBuffer) srcByteBuffer).address();
            unsafe.copyMemory(null, bbAddress + position, dstArray, INT_ARRAY_OFFSET, countInts * Integer.BYTES);
        } else {
            unsafe.copyMemory(srcByteBuffer.array(), BYTE_ARRAY_OFFSET + position, dstArray, INT_ARRAY_OFFSET, countInts * Integer.BYTES);
        }


    }

    public static void unsafeCopyIntArrayToBuffer(int[] srcArray, ByteBuffer dstByteBuffer, int position, int countInts) {

        if (dstByteBuffer.isDirect()) {
            long bbAddress = ((DirectBuffer) dstByteBuffer).address();
            unsafe.copyMemory(srcArray, INT_ARRAY_OFFSET, null, bbAddress + position, countInts * Integer.BYTES);
        } else {
            unsafe.copyMemory(srcArray, INT_ARRAY_OFFSET, dstByteBuffer.array(), BYTE_ARRAY_OFFSET + position, countInts * Integer.BYTES);
        }
    }

    private static long LONG_INT_MASK_0 = (1L << 32) - 1;
    private static long LONG_INT_MASK_1 = LONG_INT_MASK_0 << 32;

    /**
     * Combines two integers into one long where the first argument is placed in the higher four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static long intsToLongReverse(int i1, int i2) {
        return (((long) i2) & LONG_INT_MASK_0) | ((((long) i1) << 32) & LONG_INT_MASK_1);
    }

    /**
     * Breaks a long value into two integers where the first integer is taken form the higher four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static int[] longToIntsReverse(long l) {
        return new int[] {
                (int) ((l >> 32) & LONG_INT_MASK_0),
                (int) (l & LONG_INT_MASK_0),
        };
    }

    /**
     * Combines two integers into one long where the first argument is placed in the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static long intsToLongNative(int i1, int i2) {
        return (((long) i1) & LONG_INT_MASK_0) | ((((long) i2) << 32) & LONG_INT_MASK_1);
    }

    /**
     * Breaks a long value into two integers where the first integer is taken form the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static int[] longToIntsNative(long l) {
        return new int[] {
                (int) (l & LONG_INT_MASK_0),
                (int) ((l >> 32) & LONG_INT_MASK_0),
        };
    }
}
