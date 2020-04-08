package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

final class UnsafeUtils {

    static Unsafe unsafe;

    // static constructor - access and create a new instance of Unsafe
    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private UnsafeUtils() {
    }

    private static long LONG_INT_MASK_0 = (1L << 32) - 1;
    private static long LONG_INT_MASK_1 = LONG_INT_MASK_0 << 32;

    /**
     * Combines two integers into one long where the first argument is placed in the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static long intsToLong(int i1, int i2) {
        return (((long) i1) & LONG_INT_MASK_0) | ((((long) i2) << 32) & LONG_INT_MASK_1);
    }

    /**
     * Breaks a long value into two integers where the first integer is taken form the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static int[] longToInts(long l) {
        return new int[] {
                (int) (l & LONG_INT_MASK_0),
                (int) ((l >> 32) & LONG_INT_MASK_0),
        };
    }
}
