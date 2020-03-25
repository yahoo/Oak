package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.ValueUtilsImpl.LockStates.DELETED;
import static com.oath.oak.ValueUtilsImpl.LockStates.FREE;
import static com.oath.oak.ValueUtilsImpl.LockStates.LOCKED;
import static com.oath.oak.ValueUtilsImpl.LockStates.MOVED;
import static com.oath.oak.ValueUtils.ValueResult.FALSE;
import static com.oath.oak.ValueUtils.ValueResult.RETRY;
import static com.oath.oak.ValueUtils.ValueResult.TRUE;

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

    private static boolean CAS(Slice s, int expectedLock, int newLock, int version) {
        ByteBuffer buff = s.getByteBuffer();

        // Since the writing is done directly to the memory, the endianness of the memory is important here.
        // Therefore, we make sure that the values are read and written correctly.
        long expected = UnsafeUtils.intsToLong(version, expectedLock);
        long value = UnsafeUtils.intsToLong(version, newLock);
        return unsafe.compareAndSwapLong(null,
                ((DirectBuffer) buff).address() + buff.position(), expected, value);
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
    public <V> ValueResult put(Chunk<?, V> chunk, EntrySet.LookUp lookUp, V newVal, OakSerializer<V> serializer,
        MemoryManager memoryManager, InternalOakMap internalOakMap) {
        Slice s = lookUp.valueSlice;
        ValueResult result = lockWrite(s, lookUp.version);
        if (result != TRUE) {
            return result;
        }
        s = innerPut(chunk, lookUp, newVal, serializer, memoryManager, internalOakMap);
        // in case move happened: a new slice can be returned or alternatively
        // (returned slice is null) then rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        unlockWrite(s!=null ? s : lookUp.valueSlice);
        return (s!=null ? TRUE : RETRY);
    }

    private <V> Slice innerPut(Chunk<?, V> chunk, EntrySet.LookUp lookUp, V newVal, OakSerializer<V> serializer,
            MemoryManager memoryManager, InternalOakMap internalOakMap) {
        Slice s = lookUp.valueSlice;
        int capacity = serializer.calculateSize(newVal);
        if (capacity + getHeaderSize() > s.getByteBuffer().remaining()) {
            return moveValue(chunk, lookUp, memoryManager, internalOakMap, newVal);
        }
        ByteBuffer bb = getValueByteBufferNoHeader(s);
        serializer.serialize(newVal, bb);
        return s;
    }

    private <V> Slice moveValue(
        Chunk<?, V> chunk, EntrySet.LookUp lookUp, MemoryManager memoryManager,
        InternalOakMap internalOakMap, V newVal) {

        Slice oldSlice = lookUp.valueSlice.duplicate();
        Slice newSlice = internalOakMap.overwriteExistingValueForMove(lookUp, newVal, chunk);
        if (newSlice == null) {
            // rebalance was needed or the entry was updated by someone else, need to retry
            return null;
        }
        // can not release the old slice or mark it moved, before the new one is updated!
        setLockState(oldSlice, MOVED);
        memoryManager.releaseSlice(oldSlice);
        return newSlice;
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
            setLockState(s, DELETED);
            // release the slice (no need to re-read it).
            memoryManager.releaseSlice(s);
            return Result.withValue(v);
        }
    }

    @Override
    public <V> Result<V> exchange(Chunk<?, V> chunk, EntrySet.LookUp lookUp, V value,
        Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
        MemoryManager memoryManager, InternalOakMap internalOakMap) {
        ValueResult result = lockWrite(lookUp.valueSlice, lookUp.version);
        if (result != TRUE) {
            return Result.withFlag(result);
        }
        V oldValue = null;
        if (valueDeserializeTransformer != null) {
            oldValue = valueDeserializeTransformer.apply(getValueByteBufferNoHeader(lookUp.valueSlice));
        }
        Slice s = innerPut(chunk, lookUp, value, serializer, memoryManager, internalOakMap);
        // in case move happened: a new slice can be returned or alternatively
        // (returned slice is null) then rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        unlockWrite(s!=null ? s : lookUp.valueSlice);
        return s != null ? Result.withValue(oldValue) : Result.withFlag(RETRY);
    }

    @Override
    public <V> ValueResult compareExchange(Chunk<?, V> chunk, EntrySet.LookUp lookUp, V expected, V value,
        Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
        MemoryManager memoryManager, InternalOakMap internalOakMap) {
        ValueResult result = lockWrite(lookUp.valueSlice, lookUp.version);
        if (result != TRUE) {
            return result;
        }
        V oldValue = valueDeserializeTransformer.apply(getValueByteBufferNoHeader(lookUp.valueSlice));
        if (!oldValue.equals(expected)) {
            unlockWrite(lookUp.valueSlice);
            return FALSE;
        }
        Slice s = innerPut(chunk, lookUp, value, serializer, memoryManager, internalOakMap);
        // in case move happened: a new slice can be returned or alternatively
        // (returned slice is null) then rebalance might be needed
        // or the entry might be updated by someone else, need to retry
        unlockWrite(s!=null ? s : lookUp.valueSlice);
        return s != null ? TRUE : RETRY;
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
            int oldVersion = getOffHeapVersion(s);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            lockState = getLockState(s);
            if (oldVersion != getOffHeapVersion(s)) {
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
            lockState = getLockState(s);
            assert lockState > MOVED.value;
            lockState &= ~LOCK_MASK;
        } while (!CAS(s, lockState, lockState - (1 << LOCK_SHIFT), version));
        return TRUE;
    }

    @Override
    public ValueResult lockWrite(Slice s, int version) {
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getOffHeapVersion(s);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            int lockState = getLockState(s);
            if (oldVersion != getOffHeapVersion(s)) {
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
        setLockState(s, FREE);
        return TRUE;
    }

    @Override
    public ValueResult deleteValue(Slice s, int version) {
        assert version > INVALID_VERSION;
        do {
            int oldVersion = getOffHeapVersion(s);
            if (oldVersion != version) {
                return ValueResult.RETRY;
            }
            int lockState = getLockState(s);
            if (oldVersion != getOffHeapVersion(s)) {
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
        int oldVersion = getOffHeapVersion(s);
        if (oldVersion != version) {
            return ValueResult.RETRY;
        }
        int lockState = getLockState(s);
        if (oldVersion != getOffHeapVersion(s)) {
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

    private void setVersion(Slice s) {
        putInt(s, 0, s.getVersion());
    }

    private int getLockState(Slice s) {
        return getInt(s, getLockLocation());
    }

    private void setLockState(Slice s, LockStates state) {
        putInt(s, getLockLocation(), state.value);
    }

    @Override
    public void initHeader(Slice s) {
        initHeader(s, FREE);
    }

    @Override
    public void initLockedHeader(Slice s) {
        initHeader(s, LOCKED);
    }

    private void initHeader(Slice s, ValueUtilsImpl.LockStates state) {
        setVersion(s);
        setLockState(s, state);
    }

    private int getInt(Slice s, int index) {
        ByteBuffer buff = s.getByteBuffer();
        return unsafe.getInt(((DirectBuffer) buff).address() + buff.position() + index);
    }

    private void putInt(Slice s, int index, int value) {
        ByteBuffer buff = s.getByteBuffer();
        unsafe.putInt(((DirectBuffer) buff).address() + buff.position() + index, value);
    }
}
