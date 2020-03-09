package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

public final class UnsafeUtils {

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

    private static long LONG_INT_MASK_0 = (1L << Integer.SIZE) - 1L;
    private static long LONG_INT_MASK_1 = LONG_INT_MASK_0 << Integer.SIZE;

    /**
     * Combines two integers into one long where the first argument is placed in the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static long intsToLong(int i1, int i2) {
        return (((long) i1) & LONG_INT_MASK_0) | ((((long) i2) << Integer.SIZE) & LONG_INT_MASK_1);
    }
}
