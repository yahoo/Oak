package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;
import java.util.function.Function;

public class ValueUtils {

    private static final int LOCK_MASK = 0x3;
    private static final int LOCK_FREE = 0;
    static final int LOCK_LOCKED = 1;
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

    //One duplication
    static ByteBuffer getActualValueBufferLessDuplications(ByteBuffer bb) {
        bb.position(bb.position() + VALUE_HEADER_SIZE);
        ByteBuffer dup = bb.slice();
        bb.position(bb.position() - VALUE_HEADER_SIZE);
        return dup;
    }

    // Two duplications
    static ByteBuffer getActualValueBuffer(ByteBuffer bb) {
        ByteBuffer dup = bb.duplicate();
        dup.position(dup.position() + VALUE_HEADER_SIZE);
        return dup.slice();
    }

    private static boolean CAS(ByteBuffer bb, int expected, int value) {
        // assuming big endian
        assert bb.order() == ByteOrder.BIG_ENDIAN;
        return unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address() + bb.position(), reverseBytes(expected), reverseBytes(value));
    }

    /* Header Utils */

    static boolean isValueDeleted(Slice s) {
        ByteBuffer bb = s.getByteBuffer();
        return isValueDeleted(bb.getInt(bb.position()));
    }

    private static boolean isValueDeleted(int header) {
        return header == LOCK_DELETED;
    }

    static boolean lockRead(Slice s) {
        return lockRead(s.getByteBuffer());
    }

    static boolean lockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
            oldHeader &= ~LOCK_MASK;
        } while (!CAS(bb, oldHeader, oldHeader + 4));
        return true;
    }

    static void unlockRead(Slice s) {
        unlockRead(s.getByteBuffer());
    }

    static void unlockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
        } while (!CAS(bb, oldHeader, oldHeader - 4));
    }

    static boolean lockWrite(Slice s) {
        return lockWrite(s.getByteBuffer());
    }

    static boolean lockWrite(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
        } while (!CAS(bb, LOCK_FREE, LOCK_LOCKED));
        return true;
    }

    static void unlockWrite(Slice s) {
        unlockWrite(s.getByteBuffer());
    }

    static void unlockWrite(ByteBuffer bb) {
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

    static <T> T transform(ByteBuffer bb, Function<ByteBuffer, T> transformer) {
        if (!lockRead(bb)) return null;

        T transformation = transformer.apply(getActualValueBuffer(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return transformation;
    }

    static <T> T transform(Slice s, Function<ByteBuffer, T> transformer) {
        ByteBuffer bb = s.getByteBuffer();
        if (!lockRead(bb)) return null;

        T transformation = transformer.apply(getActualValueBuffer(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return transformation;
    }

    public static <V> boolean put(Slice s, V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        ByteBuffer bb = s.getByteBuffer();
        if (!lockWrite(bb)) return false;
        int capacity = serializer.calculateSize(newVal);
        if (bb.remaining() < capacity) { // can not reuse the existing space
            memoryManager.releaseSlice(s);
            s = memoryManager.allocateSlice(capacity + VALUE_HEADER_SIZE);
            bb = s.getByteBuffer();
            throw new UnsupportedOperationException();
            // TODO: update the relevant entry
        }
        ByteBuffer dup = getActualValueBuffer(bb);
        // It seems like I have to change bb here or else I expose the header to the user
        // Duplicating bb instead
        serializer.serialize(newVal, dup);
        unlockWrite(bb);
        return true;
    }

    // Why do we use finally here and not in put for example
    static boolean compute(Slice s, Consumer<OakWBuffer> computer) {
        ByteBuffer bb = s.getByteBuffer();
        if (!lockWrite(bb)) return false;
        OakWBuffer wBuffer = new OakWBufferImpl(bb);
        computer.accept(wBuffer);
        unlockWrite(bb);
        return true;
    }

    static boolean remove(Slice s, MemoryManager memoryManager) {
        ByteBuffer bb = s.getByteBuffer();
        if (!deleteValue(bb)) return false;
        // releasing the actual value and not the header
        Slice sDup = new Slice(s.getBlockID(), getActualValueBuffer(bb));
        memoryManager.releaseSlice(sDup);
        return true;
    }

    /**
     * Applies a transformation under writers locking
     *
     * @param transformer transformation to apply
     * @return Transformation result or null if value is deleted
     */
    static <T> T mutatingTransform(Slice s, Function<ByteBuffer, T> transformer) {
        T result;
        ByteBuffer bb = s.getByteBuffer();
        if (!lockWrite(bb))
            // finally clause will handle unlock
            return null;
        result = transformer.apply(getActualValueBuffer(bb));
        unlockWrite(bb);
        return result;
    }
}
