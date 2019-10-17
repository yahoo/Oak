package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.NovaManager.INVALID_VERSION;
import static com.oath.oak.NovaManager.NOVA_HEADER_SIZE;
import static com.oath.oak.NovaValueOperationsImpl.LockStates.DELETED;
import static com.oath.oak.NovaValueOperationsImpl.LockStates.FREE;
import static com.oath.oak.NovaValueOperationsImpl.LockStates.LOCKED;
import static com.oath.oak.NovaValueOperationsImpl.LockStates.MOVED;
import static com.oath.oak.NovaValueUtils.Result.FALSE;
import static com.oath.oak.NovaValueUtils.Result.RETRY;
import static com.oath.oak.NovaValueUtils.Result.TRUE;
import static com.oath.oak.UnsafeUtils.intsToLong;
import static java.lang.Long.reverseBytes;

public class NovaValueOperationsImpl implements NovaValueOperations {
    enum LockStates {
        FREE(0), LOCKED(1), DELETED(2), MOVED(3);

        public final int value;

        LockStates(int value) {
            this.value = value;
        }
    }

    private static final int LOCK_MASK = 0x3;
    private static final int LOCK_SHIFT = 2;
    private static final int VALUE_HEADER_SIZE = 4;

    private static Unsafe unsafe = UnsafeUtils.unsafe;

    private boolean CAS(Slice s, int expectedLock, int newLock, int version) {
        long expected = intsToLong(version, expectedLock);
        long value = intsToLong(version, newLock);
        return unsafe.compareAndSwapLong(null,
                ((DirectBuffer) s.getByteBuffer()).address() + s.getByteBuffer().position(), reverseBytes(expected),
                reverseBytes(value));
    }

    @Override
    public void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    @Override
    public <T> AbstractMap.SimpleEntry<Result, T> transform(Slice s, Function<ByteBuffer, T> transformer,
                                                            int version) {
        Result result = lockRead(s, version);
        if (result != TRUE) {
            return new AbstractMap.SimpleEntry<>(result, null);
        }

        T transformation = transformer.apply(getValueByteBufferNoHeader(s).asReadOnlyBuffer());
        unlockRead(s, version);
        return new AbstractMap.SimpleEntry<>(TRUE, transformation);
    }

    @Override
    public <K, V> Result put(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                             NovaManager memoryManager) {
        Result result = lockWrite(lookUp.valueSlice, lookUp.version);
        if (result != TRUE) {
            return result;
        }
        Slice s = innerPut(chunk, lookUp, newVal, serializer, memoryManager);
        unlockWrite(s);
        return TRUE;
    }

    private <K, V> Slice innerPut(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                                  NovaManager memoryManager) {
        Slice s = lookUp.valueSlice;
        int capacity = serializer.calculateSize(newVal);
        if (capacity + getHeaderSize() > s.getByteBuffer().remaining()) {
            s = moveValue(chunk, lookUp, capacity, memoryManager);
        }
        ByteBuffer bb = getValueByteBufferNoHeader(s);
        serializer.serialize(newVal, bb);
        return s;
    }

    private <K, V> Slice moveValue(Chunk<K, V> chunk, Chunk.LookUp lookUp, int capacity, NovaManager memoryManager) {
        Slice s = lookUp.valueSlice;
        putInt(s, getLockLocation(), MOVED.value);
        memoryManager.releaseSlice(s);
        s = memoryManager.allocateSlice(capacity + getHeaderSize(), false);
        putInt(s, getLockLocation(), LOCKED.value);
        int valueBlockAndLength =
                (s.getBlockID() << VALUE_BLOCK_SHIFT) | ((capacity + getHeaderSize()) & VALUE_LENGTH_MASK);
        assert chunk.casEntriesArrayLong(lookUp.entryIndex, Chunk.OFFSET.VALUE_REFERENCE, lookUp.valueReference,
                UnsafeUtils.intsToLong(valueBlockAndLength, s.getByteBuffer().position()));
        assert chunk.casEntriesArrayInt(lookUp.entryIndex, Chunk.OFFSET.VALUE_VERSION, lookUp.version, getInt(s, 0));
        return s;
    }

    @Override
    public Result compute(Slice s, Consumer<OakWBuffer> computer, int version) {
        Result result = lockWrite(s, version);
        if (result != TRUE) {
            return result;
        }
        computer.accept(new OakWBufferImpl(s, this));
        unlockWrite(s);
        return TRUE;
    }

    @Override
    public <V> AbstractMap.SimpleEntry<Result, V> remove(Slice s, NovaManager memoryManager, int version, V oldValue,
                                                         Function<ByteBuffer, V> transformer) {
        // No need to check the old value
        if (oldValue == null) {
            // try to delete
            Result result = deleteValue(s, version);
            if (result != TRUE) {
                return new AbstractMap.SimpleEntry<>(result, null);
            }
            // read the old value (the slice is not reclaimed yet)
            V v = transformer != null ? transformer.apply(getValueByteBufferNoHeader(s).asReadOnlyBuffer()) : null;
            memoryManager.releaseSlice(s);
            return new AbstractMap.SimpleEntry<>(TRUE, v);
        } else {
            // We first have to read the oldValue and only then decide whether it should be deleted.
            Result result = lockWrite(s, version);
            if (result != TRUE) {
                return new AbstractMap.SimpleEntry<>(result, null);
            }
            V v = transformer.apply(getValueByteBufferNoHeader(s).asReadOnlyBuffer());
            if (!oldValue.equals(v)) {
                unlockWrite(s);
                return new AbstractMap.SimpleEntry<>(FALSE, null);
            }
            // value is now deleted
            putInt(s, getLockLocation(), DELETED.value);
            memoryManager.releaseSlice(s);
            return new AbstractMap.SimpleEntry<>(TRUE, v);
        }
    }

    @Override
    public <K, V> AbstractMap.SimpleEntry<Result, V> exchange(Chunk<K, V> chunk, Chunk.LookUp lookUp, V value,
                                                              Function<ByteBuffer, V> valueDeserializeTransformer,
                                                              OakSerializer<V> serializer,
                                                              NovaManager memoryManager) {
        Result result = lockWrite(lookUp.valueSlice, lookUp.version);
        if (result != TRUE) {
            return new AbstractMap.SimpleEntry<>(result, null);
        }
        V oldValue = null;
        if (valueDeserializeTransformer != null) {
            oldValue = valueDeserializeTransformer.apply(getValueByteBufferNoHeader(lookUp.valueSlice));
        }
        Slice s = innerPut(chunk, lookUp, value, serializer, memoryManager);
        unlockWrite(s);
        return new AbstractMap.SimpleEntry<>(TRUE, oldValue);
    }

    @Override
    public <K, V> Result compareExchange(Chunk<K, V> chunk, Chunk.LookUp lookUp, V expected, V value,
                                         Function<ByteBuffer, V> valueDeserializeTransformer,
                                         OakSerializer<V> serializer, NovaManager memoryManager) {
        Result result = lockWrite(lookUp.valueSlice, lookUp.version);
        if (result != TRUE) {
            return result;
        }
        V oldValue = valueDeserializeTransformer.apply(getValueByteBufferNoHeader(lookUp.valueSlice));
        if (!oldValue.equals(expected)) {
            unlockWrite(lookUp.valueSlice);
            return FALSE;
        }
        Slice s = innerPut(chunk, lookUp, value, serializer, memoryManager);
        unlockWrite(s);
        return TRUE;
    }

    @Override
    public int getHeaderSize() {
        return getLockSize() + getLockLocation();
    }

    @Override
    public int getLockLocation() {
        return NOVA_HEADER_SIZE;
    }

    @Override
    public int getLockSize() {
        return VALUE_HEADER_SIZE;
    }

    @Override
    public ByteBuffer getValueByteBufferNoHeaderPrivate(Slice s) {
        ByteBuffer bb = s.getByteBuffer();
        bb.position(bb.position() + getHeaderSize());
        ByteBuffer dup = bb.slice();
        bb.position(bb.position() - getHeaderSize());
        return dup;
    }

    @Override
    public ByteBuffer getValueByteBufferNoHeader(Slice s) {
        ByteBuffer dup = s.getByteBuffer().duplicate();
        dup.position(dup.position() + getHeaderSize());
        return dup.slice();
    }

    @Override
    public Result lockRead(Slice s, int version) {
        int lockState;
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getInt(s, 0);
            if (oldVersion != version) {
                return Result.RETRY;
            }
            lockState = getInt(s, getLockLocation());
            if (oldVersion != getInt(s, 0)) {
                return Result.RETRY;
            }
            if (lockState == DELETED.value) {
                return Result.FALSE;
            }
            if (lockState == MOVED.value) {
                return RETRY;
            }
            lockState &= ~LOCK_MASK;
        } while (!CAS(s, lockState, lockState + (1 << LOCK_SHIFT), version));
        return TRUE;
    }

    @Override
    public Result unlockRead(Slice s, int version) {
        int lockState;
        assert version > INVALID_VERSION;
        do {
            lockState = getInt(s, getLockLocation());
            assert lockState > MOVED.value;
            lockState &= ~LOCK_MASK;
        } while (!CAS(s, lockState, lockState - (1 << LOCK_SHIFT), version));
        return TRUE;
    }

    @Override
    public Result lockWrite(Slice s, int version) {
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getInt(s, 0);
            if (oldVersion != version) {
                return Result.RETRY;
            }
            int lockState = getInt(s, getLockLocation());
            if (oldVersion != getInt(s, 0)) {
                return Result.RETRY;
            }
            if (lockState == DELETED.value) {
                return Result.FALSE;
            }
            if (lockState == MOVED.value) {
                return RETRY;
            }
        } while (!CAS(s, FREE.value, LOCKED.value, version));
        return TRUE;
    }

    @Override
    public Result unlockWrite(Slice s) {
        putInt(s, getLockLocation(), FREE.value);
        return TRUE;
    }

    @Override
    public Result deleteValue(Slice s, int version) {
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getInt(s, 0);
            if (oldVersion != version) {
                return Result.RETRY;
            }
            int lockState = getInt(s, getLockLocation());
            if (oldVersion != getInt(s, 0)) {
                return Result.RETRY;
            }
            if (lockState == DELETED.value) {
                return Result.FALSE;
            }
            if (lockState == MOVED.value) {
                return RETRY;
            }
        } while (!CAS(s, FREE.value, DELETED.value, version));
        return TRUE;
    }

    @Override
    public Result isValueDeleted(Slice s, int version) {
        int oldVersion = getInt(s, 0);
        if (oldVersion != version) {
            return Result.RETRY;
        }
        int lockState = getInt(s, getLockLocation());
        if (oldVersion != getInt(s, 0)) {
            return Result.RETRY;
        }
        if (lockState == MOVED.value) {
            return Result.RETRY;
        }
        if (lockState == DELETED.value) {
            return TRUE;
        }
        return Result.FALSE;
    }

    @Override
    public int getOffHeapVersion(Slice s) {
        return getInt(s, 0);
    }

    private int getInt(Slice s, int index) {
        return s.getByteBuffer().getInt(s.getByteBuffer().position() + index);
    }

    private void putInt(Slice s, int index, int value) {
        s.getByteBuffer().putInt(s.getByteBuffer().position() + index, value);
    }
}
