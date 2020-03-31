package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ValueUtils {

    int VERSION_SIZE = 4;
    int INVALID_VERSION = 0;

    enum ValueResult {
        TRUE, FALSE, RETRY

    }

    /**
     * Some implementations of values which reside in the off-heap may have a header to it (e.g., the implementation of
     * Nova Values has a header of 8 bytes).
     */
    int getHeaderSize();

    /**
     * Some implementations of values which reside may have a lock in their header.
     */
    int getLockLocation();

    int getLockSize();

    /**
     * This method only works when the byte buffer is thread local, i.e., not reachable by other threads.
     * Using this assumption, one can save one object allocation.
     *
     * @param s the Slice of the value
     * @return the ByteBuffer of the value without its header.
     */
    ByteBuffer getValueByteBufferNoHeaderPrivate(Slice s);

    /**
     * Similar to #getValueByteBufferNoHeaderPrivate(Slice), without the assumption of a thread local
     * environment.
     * It is used when calling user's function in a concurrent setting.
     *
     * @param s the Slice of the value
     * @return the ByteBuffer of the value without its header.
     */
    ByteBuffer getValueByteBufferNoHeader(Slice s);

    /**
     * Acquires a read lock
     *
     * @param s       the value Slice (including the header)
     * @param version the expected version of the value pointed by {@code s}
     * @return {@code TRUE} if the read lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult lockRead(Slice s, int version);

    /**
     * Releases a read lock
     *
     * @param s       the value Slice (including the header)
     * @param version the expected version of the value pointed by {@code s}
     * @return {@code TRUE} if the read lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult unlockRead(Slice s, int version);

    /**
     * Acquires a write lock
     *
     * @param s       the value Slice (including the header)
     * @param version the expected version of the value pointed by {@code s}
     * @return {@code TRUE} if the write lock was acquires successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult lockWrite(Slice s, int version);

    /**
     * Releases a write lock
     * Since a write lock is exclusive (unlike read lock), there is no way for the version to change, so no need to
     * pass it.
     *
     * @param s the value Slice (including the header)
     * @return {@code TRUE} if the write lock was released successfully
     * {@code FALSE} if the value is marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult unlockWrite(Slice s);

    /**
     * Marks the value pointed by {@code s} as deleted only if the version of that value matches {@code version}.
     *
     * @param s       the value Slice (including the header)
     * @param version the expected version of the value pointed by {@code s}
     * @return {@code TRUE} if the value was marked successfully
     * {@code FALSE} if the value is already marked as deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult deleteValue(Slice s, int version);

    /**
     * @param s       the value Slice (including the header)
     * @param version the expected version of the value pointed by {@code s}
     * @return {@code TRUE} if the value is marked
     * {@code FALSE} if the value is not marked
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     */
    ValueResult isValueDeleted(Slice s, int version);

    /**
     * @param s the value Slice (including the header)
     * @return the version of the value pointed by {@code s}
     */
    int getOffHeapVersion(Slice s);

    /**
     * Initializing the header version.
     * May also set other members in the header to their default values.
     * @param s the value Slice (including the header), includes the version
     */
    void initHeader(Slice s);

    /**
     * Initializing the header version and lock to be locked.
     * May also set other members in the header to their default values.
     * @param s the value Slice (including the header), includes the version
     */
    void initLockedHeader(Slice s) ;

    /* ==================== More complex methods on off-heap values ==================== */

    /**
     * Used to try and read a value off-heap
     *
     * @param s           the value Slice (including the header)
     * @param transformer value deserializer
     * @param version     the expected version of the value pointed by {@code s}
     * @param <T>         the type of {@code transformer}'s output
     * @return {@code TRUE} if the value was read successfully
     * {@code FALSE} if the value is deleted
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     * In case of {@code TRUE}, the read value is stored in the returned Result, otherwise, the value is {@code null}.
     */
    <T> Result<T> transform(Slice s, Function<ByteBuffer, T> transformer, int version);

    /**
     * @see #exchange(Chunk, EntrySet.LookUp, Object, Function, OakSerializer, MemoryManager, InternalOakMap)
     * Does not return the value previously written off-heap
     */
    <V> ValueResult put(Chunk<?, V> chunk, EntrySet.LookUp lookUp, V newVal, OakSerializer<V> serializer,
        MemoryManager memoryManager, InternalOakMap internalOakMap);

    /**
     * @param s        the value Slice (including the header)
     * @param computer the function to apply on the Slice
     * @param version  the expected version of the value pointed by {@code s}
     * @return {@code TRUE} if the function was applied successfully,
     * {@code FAILURE} if the value is deleted,
     * {@code RETRY} if the value was moved.
     */
    ValueResult compute(Slice s, Consumer<OakWBuffer> computer, int version);

    /**
     * Marks a value as deleted and frees its slice (whether the header is freed or not is implementation dependant).
     *
     * @param s             the value Slice (including the header)
     * @param version       the expected version of the value pointed by {@code s}
     * @param memoryManager the memory manager to which the slice is returned to
     * @param oldValue      in case of a conditional remove, this is the value to which the actual value is compared to
     * @param transformer   value deserializer
     * @param <V>           the type of the value
     * @return {@code TRUE} if the value was removed successfully,
     * {@code FALSE} if the value is already deleted, or if it does not equal to {@code oldValue}
     * {@code RETRY} if the value was moved, or the version of the off-heap value does not match {@code version}.
     * In case of success, the value of the returned Result is the value which was written in the off-heap before the
     * removal (if {@code transformer} is not null), otherwise, it is {@code null}.
     */
    <V> Result<V> remove(Slice s, MemoryManager memoryManager, int version, V oldValue,
                         Function<ByteBuffer, V> transformer);

    /**
     * Replaces the value written in the Slice referenced by {@code lookUp} with {@code value}.
     * {@code chuck} is used iff {@code newValue} takes more space than the old value does, meaning it has to move.
     * If the value moves, the old slice is marked as moved and freed.
     *
     * @param <V>                         the type of the value
     * @param chunk                       the chunk with the entry to which the value is linked to
     * @param lookUp                      has the Slice of the value and the entry index
     * @param value                       the new value to write
     * @param valueDeserializeTransformer used to read the previous value
     * @param serializer                  value serializer to write {@code newValue}
     * @param memoryManager               the memory manager to free a slice with is not needed after the value moved
     * @param internalOakMap
     * @return {@code TRUE} if the value was written off-heap successfully
     * {@code FALSE} if the value is deleted (cannot be overwritten)
     * {@code RETRY} if the value was moved, if the chuck is frozen/released (prevents the moving of the value), or
     * if the version of the value does not match the version written inside {@code lookUp}.
     * Along side the flag of the result, in case the exchange succeeded, it also returns the value that
     * was written before the exchange.
     */
    <V> Result<V> exchange(Chunk<?, V> chunk, EntrySet.LookUp lookUp, V value, Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
        MemoryManager memoryManager, InternalOakMap internalOakMap);

    /**
     * @param expected the old value to which we compare the current value
     * @param internalOakMap
     * @return {@code TRUE} if the exchange went successfully
     * {@code FAILURE} if the value is deleted or if the actual value referenced in {@code lookUp} does not equal to
     * {@code expected}
     * {@code RETRY} for the same reasons as exchange
     * @see #exchange(Chunk, EntrySet.LookUp, Object, Function, OakSerializer, MemoryManager, InternalOakMap)
     */
    <V> ValueResult compareExchange(Chunk<?, V> chunk, EntrySet.LookUp lookUp, V expected, V value,
        Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
        MemoryManager memoryManager, InternalOakMap internalOakMap);
}
