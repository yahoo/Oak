package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.ValueUtils.LockStates.*;
import static com.oath.oak.ValueUtils.ValueResult.*;
import static java.lang.Integer.reverseBytes;

// All of the methods are atomic (this atomicity is achieved using read/write locks)
public class ValueUtils {

    enum LockStates {
        FREE(0), LOCKED(1), DELETED(2), MOVED(3);

        public int value;

        LockStates(int value) {
            this.value = value;
        }
    }

    enum ValueResult {
        SUCCESS, FAILURE, RETRY
    }

    private static final int LOCK_MASK = 0x3;
    public static final int VALUE_HEADER_SIZE = 4;

    private static Unsafe unsafe = UnsafeUtils.unsafe;

    //One duplication
    //Instead of duplicating the buffer and then slicing it, since it is a private environment, no need to duplicate

    /**
     * This method only works when the byte buffer is thread local, i.e., not reachable by other threads.
     * Using this assumption, we save one object creation.
     *
     * @param bb the Byte Buffer of the value
     * @return the byte buffer of the value without its header.
     */
    static ByteBuffer getValueByteBufferNoHeaderPrivate(ByteBuffer bb) {
        bb.position(bb.position() + VALUE_HEADER_SIZE);
        ByteBuffer dup = bb.slice();
        bb.position(bb.position() - VALUE_HEADER_SIZE);
        return dup;
    }

    // Two duplications

    /**
     * Similar to #getValueByteBufferNoHeaderPrivate(ByteBuffer bb), without the assumption of a thread local
     * environment.
     * Creates two new objects instead of one.
     *
     * @param bb the Byte Buffer of the value
     * @return the byte buffer of the value without its header.
     */
    static ByteBuffer getValueByteBufferNoHeader(ByteBuffer bb) {
        ByteBuffer dup = bb.duplicate();
        dup.position(dup.position() + VALUE_HEADER_SIZE);
        return dup.slice();
    }

    /**
     * @param s a value Slice (including the header)
     * @return {@code true} if the value is deleted (the header equals to DELETED.value, 2)
     */
    static boolean isValueDeleted(Slice s) {
        ByteBuffer bb = s.getByteBuffer();
        return isValueDeleted(bb.getInt(bb.position()));
    }

    private static boolean CAS(ByteBuffer bb, int expected, int value) {
        // assuming big endian
        assert bb.order() == ByteOrder.BIG_ENDIAN;
        return unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address() + bb.position(), reverseBytes(expected),
                reverseBytes(value));
    }

    private static boolean isValueDeleted(int header) {
        return header == DELETED.value;
    }

    private static boolean wasValueMoved(int header) {
        return header == MOVED.value;
    }

    /**
     * @see #lockRead(ByteBuffer)
     */
    static ValueResult lockRead(Slice s) {
        return lockRead(s.getByteBuffer());
    }

    /**
     * Acquires a read lock
     *
     * @param bb the value Byte Buffer (including the header)
     * @return {@code SUCCESS} if the read lock was acquires successfully
     * {@code FAILURE} if the value is marked as deleted
     * {@code RETRY} if the value was moved
     */
    static ValueResult lockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) {
                return FAILURE;
            }
            if (wasValueMoved(oldHeader)) {
                return RETRY;
            }
            oldHeader &= ~LOCK_MASK;
        } while (!CAS(bb, oldHeader, oldHeader + 4));
        return SUCCESS;
    }

    /**
     * @see #unlockRead(ByteBuffer)
     */
    static void unlockRead(Slice s) {
        unlockRead(s.getByteBuffer());
    }

    /**
     * Releases a read lock of the given value.
     *
     * @param bb the value Byte Buffer (including the header)
     */
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
            if (isValueDeleted(oldHeader)) {
                return FAILURE;
            }
            if (wasValueMoved(oldHeader)) {
                return RETRY;
            }
        } while (!CAS(bb, FREE.value, LOCKED.value));
        return SUCCESS;
    }

    private static void unlockWrite(Slice s) {
        unlockWrite(s.getByteBuffer());
    }

    private static void unlockWrite(ByteBuffer bb) {
        bb.putInt(bb.position(), FREE.value);
        // maybe a fence?
    }

    /* Handle Methods */

    static void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    /**
     * Used only in OakRValueBuffer
     *
     * @param bb          the value Byte Buffer (including the header)
     * @param transformer value deserializer
     * @param <T>         the type of {@code transformer}'s output
     * @return {@code null} if the value is deleted or was moved.
     * The value written off-heap is returned otherwise.
     */
    static <T> T transform(ByteBuffer bb, Function<ByteBuffer, T> transformer) {
        if (lockRead(bb) != SUCCESS) {
            return null;
        }

        T transformation = transformer.apply(getValueByteBufferNoHeader(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return transformation;
    }

    /**
     * Used to try and read a value off-heap
     *
     * @param s           the value Slice (including the header)
     * @param transformer value deserializer
     * @param <T>         the type of {@code transformer}'s output
     * @return {@code SUCCESS} if the value was read successfully,
     * {@code FAILURE} if the value is deleted,
     * {@code RETRY} if the value was moved.
     * In case of {@code SUCCESS}, the value of the returned entry is the read value, otherwise, the value is {@code
     * null}.
     */
    static <T> AbstractMap.SimpleEntry<ValueResult, T> transform(Slice s, Function<ByteBuffer, T> transformer) {
        ByteBuffer bb = s.getByteBuffer();
        ValueResult res = lockRead(bb);
        if (res != SUCCESS) {
            return new AbstractMap.SimpleEntry<>(res, null);
        }

        T transformation = transformer.apply(getValueByteBufferNoHeader(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return new AbstractMap.SimpleEntry<>(SUCCESS, transformation);
    }

    /**
     * Replaces the value written in the Slice referenced by {@code lookup} with {@code newVal}.
     * {@code chuck} is used iff {@code newValue} takes more space than the old value.
     * If the value moves, the old slice is marked as moved and freed.
     *
     * @param chunk         the chunk with the entry to which the value is linked to
     * @param lookUp        has the Slice of the value and the entry index
     * @param newVal        the new value to write
     * @param serializer    value serializer
     * @param memoryManager the memory manager to free a slice with is not needed after the value moved
     * @param <V>           the type of the value
     * @return {@code SUCCESS} if the value was written off-heap successfully,
     * {@code FAILURE} if the value is deleted (cannot be overwritten),
     * {@code RETRY} if the value was moved, or if the chuck is frozen/released (prevents the moving of the value).
     */
    public static <V> ValueResult put(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                                      MemoryManager memoryManager) {
        ValueResult res = lockWrite(lookUp.valueSlice.getByteBuffer());
        if (res != SUCCESS) {
            return res;
        }
        res = innerPut(chunk, lookUp, newVal, serializer, memoryManager);
        unlockWrite(lookUp.valueSlice.getByteBuffer());
        return res;
    }

    private static <V> ValueResult innerPut(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newVal,
                                            OakSerializer<V> serializer, MemoryManager memoryManager) {
        ByteBuffer bb = lookUp.valueSlice.getByteBuffer();
        int capacity = serializer.calculateSize(newVal);
        if (bb.remaining() < capacity + ValueUtils.VALUE_HEADER_SIZE) { // can not reuse the existing space
            if (!chunk.publish()) {
                return RETRY;
            }
            bb.putInt(bb.position(), MOVED.value);
            ByteBuffer dup = bb.duplicate();
            dup.position(dup.position() + ValueUtils.VALUE_HEADER_SIZE);
            Slice s = lookUp.valueSlice;
            Slice sDup = new Slice(s.getBlockID(), dup);
            memoryManager.releaseSlice(sDup);
            s = memoryManager.allocateSlice(capacity + VALUE_HEADER_SIZE);
            lookUp.valueSlice = s;
            bb = s.getByteBuffer();
            bb.putInt(bb.position(), LOCKED.value);
            int valueBlockAndLength =
                    (s.getBlockID() << VALUE_BLOCK_SHIFT) | ((capacity + VALUE_HEADER_SIZE) & VALUE_LENGTH_MASK);
            assert chunk.casEntriesArrayLong(lookUp.entryIndex, Chunk.OFFSET.VALUE_REFERENCE, lookUp.valueReference,
                    UnsafeUtils.intsToLong(valueBlockAndLength, bb.position()));
            chunk.unpublish();
        }
        ByteBuffer dup = getValueByteBufferNoHeader(bb);
        // It seems like I have to change bb here or else I expose the header to the user
        // Duplicating bb instead
        serializer.serialize(newVal, dup);
        return SUCCESS;
    }

    /**
     * @param s        the value Slice (including the header)
     * @param computer the function to apply on the Slice
     * @return {@code SUCCESS} if the function was applied successfully,
     * {@code FAILURE} if the value is deleted,
     * {@code RETRY} if the value was moved.
     */
    static ValueResult compute(Slice s, Consumer<OakWBuffer> computer) {
        ByteBuffer bb = s.getByteBuffer();
        ValueResult res = lockWrite(bb);
        if (res != SUCCESS) {
            return res;
        }
        OakWBuffer wBuffer = new OakWBufferImpl(bb);
        computer.accept(wBuffer);
        unlockWrite(bb);
        return SUCCESS;
    }

    /**
     * Marks a value as DELETED and frees its slice (not including the header).
     *
     * @param s             the value Slice (including the header)
     * @param memoryManager the memory manager to which the slice is returned to
     * @param oldValue      in case of a conditional remove, this is the value to which the actual value is compared to
     * @param transformer   value deserializer
     * @param <V>           the type of the value
     * @return {@code SUCCESS} if the value was removed successfully,
     * {@code FAILURE} if the value is already deleted, or if it does not equal to {@code oldValue}
     * {@code RETRY} if the value was moved.
     * In case of success, the value of the returned entry is the value which resides in the off-heap before the
     * removal (if {@code transformer} is not null), otherwise, it is {@code null}.
     */
    static <V> Map.Entry<ValueResult, V> remove(Slice s, MemoryManager memoryManager, V oldValue, Function<ByteBuffer
            , V> transformer) {
        ByteBuffer bb = s.getByteBuffer();
        ValueResult res = lockWrite(bb);
        if (res != SUCCESS) {
            return new AbstractMap.SimpleImmutableEntry<>(res, null);
        }
        // Reading the previous value
        ByteBuffer dup = bb.duplicate();
        dup.position(dup.position() + ValueUtils.VALUE_HEADER_SIZE);
        V v = null;
        if (transformer != null) {
            v = transformer.apply(dup.slice().asReadOnlyBuffer());
            if (oldValue != null && !oldValue.equals(v)) {
                unlockWrite(s);
                return new AbstractMap.SimpleImmutableEntry<>(FAILURE, null);
            }
        }
        // The previous value matches, so the slice is deleted
        bb.putInt(bb.position(), DELETED.value);
        Slice sDup = new Slice(s.getBlockID(), dup);
        memoryManager.releaseSlice(sDup);
        return new AbstractMap.SimpleImmutableEntry<>(SUCCESS, v);
    }

    /**
     * @return Along side the return value of put, in case exchange went successfully, it also returns the value that
     * was written before the exchange.
     * @see #put(Chunk, Chunk.LookUp, Object, OakSerializer, MemoryManager)
     */
    static <V> AbstractMap.SimpleEntry<ValueResult, V> exchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newValue,
                                                                Function<ByteBuffer, V> valueDeserializeTransformer,
                                                                OakSerializer<V> serializer,
                                                                MemoryManager memoryManager) {
        ValueResult result = lockWrite(lookUp.valueSlice.getByteBuffer());
        if (result != SUCCESS) {
            return new AbstractMap.SimpleEntry<>(result, null);
        }
        V v = valueDeserializeTransformer != null ?
                valueDeserializeTransformer.apply(getValueByteBufferNoHeader(lookUp.valueSlice.getByteBuffer())) : null;
        result = innerPut(chunk, lookUp, newValue, serializer, memoryManager);
        unlockWrite(lookUp.valueSlice.getByteBuffer());
        return new AbstractMap.SimpleEntry<>(result, v);
    }

    /**
     * @see #put(Chunk, Chunk.LookUp, Object, OakSerializer, MemoryManager)
     * @param oldValue the old value to which we compare the current value
     * @return {@code SUCCESS} if the exchange went successfully,
     * {@code FAILURE} if the value is deleted or if the value does not equal to {@code oldValue}
     * {@code RETRY} for the same reasons as put
     */
    static <V> ValueResult compareExchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V oldValue, V newValue,
                                           Function<ByteBuffer, V> valueDeserializeTransformer,
                                           OakSerializer<V> serializer, MemoryManager memoryManager) {
        ValueResult result = lockWrite(lookUp.valueSlice.getByteBuffer());
        if (result != SUCCESS) {
            return result;
        }
        try {
            V v = valueDeserializeTransformer.apply(getValueByteBufferNoHeader(lookUp.valueSlice.getByteBuffer()));
            if (!v.equals(oldValue)) {
                return FAILURE;
            }
            return innerPut(chunk, lookUp, newValue, serializer, memoryManager);
        } finally {
            unlockWrite(lookUp.valueSlice.getByteBuffer());
        }
    }
}
