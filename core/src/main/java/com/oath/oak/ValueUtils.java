package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.function.Function;

public class ValueHeaderUtils {

    private static final int LOCK_MASK = 0x3;
    private static final int READERS_SHIFT = 2;
    private static final int LOCK_FREE = 0;
    private static final int LOCK_LOCKED = 1;
    private static final int LOCK_DELETED = 2;

    private static Unsafe unsafe;

    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static boolean isValueDeleted(ByteBuffer bb) {
        return isValueDeleted(bb.getInt(0));

    }

    private static boolean isValueDeleted(int header) {
        return (header & LOCK_MASK) == LOCK_DELETED;
    }

    public static boolean lockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(0);
            if (isValueDeleted(oldHeader)) return false;
        } while (unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address(), oldHeader, oldHeader + 4));
        return true;
    }

    public static void unlockRead(ByteBuffer bb){
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(0);
        } while (unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address(), oldHeader, oldHeader - 4));
    }

    public static boolean lockWrite(ByteBuffer bb){
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(0);
            if (isValueDeleted(oldHeader)) return false;
        } while (unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address(), LOCK_FREE, LOCK_LOCKED));
        return true;
    }

    public static void unlockWrite(ByteBuffer bb){
        bb.putInt(LOCK_FREE);
        // maybe a fence?
    }

    public static boolean deleteValue(ByteBuffer bb){
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(0);
            if (isValueDeleted(oldHeader)) return false;
        } while (unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address(), LOCK_FREE, LOCK_DELETED));
        return true;
    }

    /* Handle Methods */

    public <T> T transform(Function<ByteBuffer, T> transformer) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            return null;
        }
        T transformation = transformer.apply(getSlicedReadOnlyByteBuffer());
        readLock.unlock();
        return transformation;
    }
}
