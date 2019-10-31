package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.ValueUtilsImpl.LockStates.DELETED;
import static com.oath.oak.ValueUtilsImpl.LockStates.FREE;
import static com.oath.oak.ValueUtilsImpl.LockStates.LOCKED;
import static com.oath.oak.ValueUtilsImpl.LockStates.MOVED;
import static com.oath.oak.ValueUtils.ValueResult.FALSE;
import static com.oath.oak.ValueUtils.ValueResult.RETRY;
import static com.oath.oak.ValueUtils.ValueResult.TRUE;
import static com.oath.oak.UnsafeUtils.intsToLong;
import static java.lang.Long.reverseBytes;

public class ValueUtilsImpl implements ValueUtils {
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
        // Since the writing is done directly to the memory, the endianness of the memory is important here.
        // Therefore, we make sure that the values are read and written correctly.
        if (s.getByteBuffer().order() == ByteOrder.BIG_ENDIAN) {
            expected = reverseBytes(expected);
            value = reverseBytes(value);
        }
        return unsafe.compareAndSwapLong(null,
                ((DirectBuffer) s.getByteBuffer()).address() + s.getByteBuffer().position(), expected, value);
    }

    @Override
    public void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    @Override
    public <T> Result<T> transform(Slice s, Function<ByteBuffer, T> transformer,
                                   int version) {
        ValueResult result = lockRead(s, version);
        if (result != TRUE) {
            return Result.withFlag(result);
        }

        T transformation = transformer.apply(getValueByteBufferNoHeader(s).asReadOnlyBuffer());
        unlockRead(s, version);
        return Result.withValue(transformation);
    }

    @Override
    public <V> ValueResult put(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                               MemoryManager memoryManager) {
        ValueResult result = lockWrite(lookUp.valueSlice, lookUp.version);
        if (result != TRUE) {
            return result;
        }
        Slice s = innerPut(chunk, lookUp, newVal, serializer, memoryManager);
        unlockWrite(s);
        return TRUE;
    }

    private <V> Slice innerPut(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                               MemoryManager memoryManager) {
        Slice s = lookUp.valueSlice;
        int capacity = serializer.calculateSize(newVal);
        if (capacity + getHeaderSize() > s.getByteBuffer().remaining()) {
            s = moveValue(chunk, lookUp, capacity, memoryManager);
        }
        ByteBuffer bb = getValueByteBufferNoHeader(s);
        serializer.serialize(newVal, bb);
        return s;
    }

    private <V> Slice moveValue(Chunk<?, V> chunk, Chunk.LookUp lookUp, int capacity, MemoryManager memoryManager) {
        Slice s = lookUp.valueSlice;
        putInt(s, getLockLocation(), MOVED.value);
        memoryManager.releaseSlice(s);
        s = memoryManager.allocateSlice(capacity + getHeaderSize(), MemoryManager.Allocate.VALUE);
        putInt(s, getLockLocation(), LOCKED.value);
        int valueBlockAndLength =
                (s.getBlockID() << VALUE_BLOCK_SHIFT) | ((capacity + getHeaderSize()) & VALUE_LENGTH_MASK);
        assert chunk.casEntriesArrayLong(lookUp.entryIndex, Chunk.OFFSET.VALUE_REFERENCE, lookUp.valueReference,
                UnsafeUtils.intsToLong(valueBlockAndLength, s.getByteBuffer().position()));
        assert chunk.casEntriesArrayInt(lookUp.entryIndex, Chunk.OFFSET.VALUE_VERSION, lookUp.version, getInt(s, 0));
        return s;
    }

    @Override
    public ValueResult compute(Slice s, Consumer<OakWBuffer> computer, int version) {
        ValueResult result = lockWrite(s, version);
        if (result != TRUE) {
            return result;
        }
        computer.accept(new OakWBufferImpl(s, this));
        unlockWrite(s);
        return TRUE;
    }

    @Override
    public <V> Result<V> remove(Slice s, MemoryManager memoryManager, int version, V oldValue,
                                Function<ByteBuffer, V> transformer) {
        // Not a conditional remove, so we can delete immediately
        if (oldValue == null) {
            // try to delete
            ValueResult result = deleteValue(s, version);
            if (result != TRUE) {
                return Result.withFlag(result);
            }
            // Now the value is deleted, and all other threads will treat it as deleted, but it is not yet freed, so
            // this thread can read from it.
            // read the old value (the slice is not reclaimed yet)
            V v = transformer != null ? transformer.apply(getValueByteBufferNoHeader(s).asReadOnlyBuffer()) : null;
            // release the slice
            memoryManager.releaseSlice(s);
            // return TRUE with the old value
            return Result.withValue(v);
        } else {
            // This is a conditional remove, so we first have to check whether the current value matches the expected
            // one.
            // We start by acquiring a write lock for reading since we do not want concurrent reads.
            ValueResult result = lockWrite(s, version);
            if (result != TRUE) {
                return Result.withFlag(result);
            }
            V v = transformer.apply(getValueByteBufferNoHeader(s).asReadOnlyBuffer());
            // This is where we check the equality between the expected value and the actual value
            if (!oldValue.equals(v)) {
                unlockWrite(s);
                return Result.withFlag(FALSE);
            }
            // both values match so the value is marked as deleted. No need for a CAS since a write lock is exclusive
            putInt(s, getLockLocation(), DELETED.value);
            // release the slice (no need to re-read it).
            memoryManager.releaseSlice(s);
            return Result.withValue(v);
        }
    }

    @Override
    public <V> Result<V> exchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V value,
                                  Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
                                  MemoryManager memoryManager) {
        ValueResult result = lockWrite(lookUp.valueSlice, lookUp.version);
        if (result != TRUE) {
            return Result.withFlag(result);
        }
        V oldValue = null;
        if (valueDeserializeTransformer != null) {
            oldValue = valueDeserializeTransformer.apply(getValueByteBufferNoHeader(lookUp.valueSlice));
        }
        Slice s = innerPut(chunk, lookUp, value, serializer, memoryManager);
        unlockWrite(s);
        return Result.withValue(oldValue);
    }

    @Override
    public <V> ValueResult compareExchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V expected, V value,
                                           Function<ByteBuffer, V> valueDeserializeTransformer,
                                           OakSerializer<V> serializer, MemoryManager memoryManager) {
        ValueResult result = lockWrite(lookUp.valueSlice, lookUp.version);
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
        return VERSION_SIZE;
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
    public ValueResult lockRead(Slice s, int version) {
        int lockState;
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getInt(s, 0);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            lockState = getInt(s, getLockLocation());
            if (oldVersion != getInt(s, 0)) {
                return ValueResult.RETRY;
            }
            if (lockState == DELETED.value) {
                return ValueResult.FALSE;
            }
            if (lockState == MOVED.value) {
                return RETRY;
            }
            lockState &= ~LOCK_MASK;
        } while (!CAS(s, lockState, lockState + (1 << LOCK_SHIFT), version));
        return TRUE;
    }

    @Override
    public ValueResult unlockRead(Slice s, int version) {
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
    public ValueResult lockWrite(Slice s, int version) {
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getInt(s, 0);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            int lockState = getInt(s, getLockLocation());
            if (oldVersion != getInt(s, 0)) {
                return ValueResult.RETRY;
            }
            if (lockState == DELETED.value) {
                return ValueResult.FALSE;
            }
            if (lockState == MOVED.value) {
                return RETRY;
            }
        } while (!CAS(s, FREE.value, LOCKED.value, version));
        return TRUE;
    }

    @Override
    public ValueResult unlockWrite(Slice s) {
        putInt(s, getLockLocation(), FREE.value);
        return TRUE;
    }

    @Override
    public ValueResult deleteValue(Slice s, int version) {
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getInt(s, 0);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            int lockState = getInt(s, getLockLocation());
            if (oldVersion != getInt(s, 0)) {
                return ValueResult.RETRY;
            }
            if (lockState == DELETED.value) {
                return ValueResult.FALSE;
            }
            if (lockState == MOVED.value) {
                return RETRY;
            }
        } while (!CAS(s, FREE.value, DELETED.value, version));
        return TRUE;
    }

    @Override
    public ValueResult isValueDeleted(Slice s, int version) {
        int oldVersion = getInt(s, 0);
        if (oldVersion != version) {
            return ValueResult.RETRY;
        }
        int lockState = getInt(s, getLockLocation());
        if (oldVersion != getInt(s, 0)) {
            return ValueResult.RETRY;
        }
        if (lockState == MOVED.value) {
            return ValueResult.RETRY;
        }
        if (lockState == DELETED.value) {
            return TRUE;
        }
        return ValueResult.FALSE;
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
