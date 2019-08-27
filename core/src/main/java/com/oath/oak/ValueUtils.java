package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.ValueUtils.LockStats.*;
import static com.oath.oak.ValueUtils.ValueResult.*;

public class ValueUtils {

    enum LockStats {
        FREE(0), LOCKED(1), DELETED(2), MOVED(3);

        public int value;

        LockStats(int value) {
            this.value = value;
        }
    }

    enum ValueResult {
        SUCCESS, FAILURE, RETRY
    }

    private static final int LOCK_MASK = 0x3;
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
        return header == DELETED.value;
    }

    private static boolean wasValueMoved(int header) {
        return header == MOVED.value;
    }

    static ValueResult lockRead(Slice s) {
        return lockRead(s.getByteBuffer());
    }

    static ValueResult lockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return FAILURE;
            if (wasValueMoved(oldHeader)) return RETRY;
            oldHeader &= ~LOCK_MASK;
        } while (!CAS(bb, oldHeader, oldHeader + 4));
        return SUCCESS;
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

    private static ValueResult lockWrite(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return FAILURE;
            if (wasValueMoved(oldHeader)) return RETRY;
        } while (!CAS(bb, FREE.value, LOCKED.value));
        return SUCCESS;
    }

    private static void unlockWrite(ByteBuffer bb) {
        bb.putInt(bb.position(), FREE.value);
        // maybe a fence?
    }

    private static ValueResult deleteValue(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return FAILURE;
            if (wasValueMoved(oldHeader)) return RETRY;
        } while (!CAS(bb, FREE.value, DELETED.value));
        return SUCCESS;
    }

    /* Handle Methods */

    static void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    static <T> T transform(ByteBuffer bb, Function<ByteBuffer, T> transformer) {
        if (lockRead(bb) != SUCCESS) return null;

        T transformation = transformer.apply(getActualValueBuffer(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return transformation;
    }

    static <T> AbstractMap.SimpleEntry<ValueResult, T> transform(Slice s, Function<ByteBuffer, T> transformer) {
        ByteBuffer bb = s.getByteBuffer();
        ValueResult res = lockRead(bb);
        if (res != SUCCESS) return new AbstractMap.SimpleEntry<>(res, null);

        T transformation = transformer.apply(getActualValueBuffer(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return new AbstractMap.SimpleEntry<>(SUCCESS, transformation);
    }

    public static <K, V> ValueResult put(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        ByteBuffer bb = lookUp.valueSlice.getByteBuffer();
        ValueResult res = lockWrite(bb);
        if (res != SUCCESS) return res;
        int capacity = serializer.calculateSize(newVal);
        if (bb.remaining() < capacity) { // can not reuse the existing space
            bb.putInt(bb.position(), MOVED.value);
            ByteBuffer dup = bb.duplicate();
            dup.position(dup.position() + 4);
            Slice s = lookUp.valueSlice;
            Slice sDup = new Slice(s.getBlockID(), dup);
            memoryManager.releaseSlice(sDup);
            s = memoryManager.allocateSlice(capacity + VALUE_HEADER_SIZE);
            bb = s.getByteBuffer();
            bb.putInt(bb.position(), LOCKED.value);
            int valueBlockAndLength = (s.getBlockID() << VALUE_BLOCK_SHIFT) | ((capacity + VALUE_HEADER_SIZE) & VALUE_LENGTH_MASK);
            assert chunk.longCasEntriesArray(lookUp.entryIndex, Chunk.OFFSET.VALUE_STATS, lookUp.valueStats, UnsafeUtils.intsToLong(valueBlockAndLength, bb.position()));
            // TODO: CAS the new slice into the entry
        }
        ByteBuffer dup = getActualValueBuffer(bb);
        // It seems like I have to change bb here or else I expose the header to the user
        // Duplicating bb instead
        serializer.serialize(newVal, dup);
        unlockWrite(bb);
        return SUCCESS;
    }

    static ValueResult compute(Slice s, Consumer<OakWBuffer> computer) {
        ByteBuffer bb = s.getByteBuffer();
        ValueResult res = lockWrite(bb);
        if (res != SUCCESS) return res;
        OakWBuffer wBuffer = new OakWBufferImpl(bb);
        computer.accept(wBuffer);
        unlockWrite(bb);
        return SUCCESS;
    }

    static ValueResult remove(Slice s, MemoryManager memoryManager) {
        ByteBuffer bb = s.getByteBuffer();
        ValueResult res = deleteValue(bb);
        if (res != SUCCESS) return res;
        // releasing the actual value and not the header
        ByteBuffer dup = bb.duplicate();
        dup.position(dup.position() + 4);
        Slice sDup = new Slice(s.getBlockID(), dup);
        memoryManager.releaseSlice(sDup);
        return SUCCESS;
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
        if (lockWrite(bb) != SUCCESS)
            // finally clause will handle unlock
            return null;
        result = transformer.apply(getActualValueBuffer(bb));
        unlockWrite(bb);
        return result;
    }
}
