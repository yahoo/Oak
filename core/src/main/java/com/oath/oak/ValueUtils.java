package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

public class ValueUtils {

    private static final int LOCK_MASK = 0x3;
    private static final int LOCK_FREE = 0;
    private static final int LOCK_LOCKED = 1;
    private static final int LOCK_DELETED = 2;
    public static final int VALUE_HEADER_SIZE = 4;

    private static Unsafe unsafe;

    private static int reverseBytes(int i) {
        int newI = 0;
        newI += (i & 0xff) << 24;
        newI += (i & 0xff00) << 16;
        newI += (i & 0xff0000) << 8;
        newI += (i & 0xff000000);
        return newI;
    }

    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static ByteBuffer getActualValueBuffer(ByteBuffer bb) {
        bb.position(bb.position() + VALUE_HEADER_SIZE);
        ByteBuffer actualValue = bb.slice();
        bb.position(bb.position() - VALUE_HEADER_SIZE);
        return actualValue;
    }

    /* Header Utils */

    public static boolean isValueDeleted(ByteBuffer bb) {
        return isValueDeleted(bb.getInt(bb.position()));

    }

    private static boolean isValueDeleted(int header) {
        return (header & LOCK_MASK) == LOCK_DELETED;
    }

    static boolean lockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
            oldHeader &= ~0x3;
        } while (!CAS(bb, oldHeader, oldHeader + 4));
        return true;
    }

    private static boolean CAS(ByteBuffer bb, int expected, int value) {
        return unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address() + bb.position(), reverseBytes(expected), reverseBytes(value));
    }

    static void unlockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
        } while (!CAS(bb, oldHeader, oldHeader - 4));
    }

    private static boolean lockWrite(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
        } while (!CAS(bb, LOCK_FREE, LOCK_LOCKED));
        return true;
    }

    private static void unlockWrite(ByteBuffer bb) {
        bb.putInt(bb.position(), LOCK_FREE);
        // maybe a fence?
    }

    private static boolean deleteValue(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
        } while (!CAS(bb, LOCK_FREE, LOCK_DELETED));
        return true;
    }

    /* Handle Methods */

    static void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    public static <T> T transform(ByteBuffer bb, Function<ByteBuffer, T> transformer) {
        if (!lockRead(bb)) return null;

        T transformation = transformer.apply(getActualValueBuffer(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return transformation;
    }

    public static <V> boolean put(ByteBuffer bb, V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        if (!lockWrite(bb)) return false;
        int capacity = serializer.calculateSize(newVal);
        if (bb.remaining() < capacity) { // can not reuse the existing space
            memoryManager.release(getActualValueBuffer(bb));
            memoryManager.allocate(capacity + VALUE_HEADER_SIZE);
            throw new RuntimeException();
            // TODO: update the relevant entry
        }
        serializer.serialize(newVal, getActualValueBuffer(bb));
        unlockWrite(bb);
        return true;
    }

    // Why do we use finally here and not in put for example
    static boolean compute(ByteBuffer bb, Consumer<OakWBuffer> computer) {
        if (!lockWrite(bb)) return false;
        OakWBuffer wBuffer = new OakWBufferImpl(getActualValueBuffer(bb));
        computer.accept(wBuffer);
        unlockWrite(bb);
        return true;
    }

    static boolean remove(ByteBuffer bb, MemoryManager memoryManager) {
        if (deleteValue(bb)) return false;
        // releasing the actual value and not the header
        memoryManager.release(getActualValueBuffer(bb));
        return true;
    }

    /**
     * Applies a transformation under writers locking
     *
     * @param transformer transformation to apply
     * @return Transformation result or null if value is deleted
     */
    public static <T> T mutatingTransform(ByteBuffer bb, Function<ByteBuffer, T> transformer) {
        T result;
        if (!lockWrite(bb))
            // finally clause will handle unlock
            return null;
        result = transformer.apply(getActualValueBuffer(bb));
        return result;
    }
}
