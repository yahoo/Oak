package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ValueUtils {

    enum ValueResult {
        TRUE, FALSE, RETRY
    }

    int getHeaderSize();

    int getLockLocation();

    int getLockSize();

    ByteBuffer getValueByteBufferNoHeaderPrivate(Slice s);

    ByteBuffer getValueByteBufferNoHeader(Slice s);

    ValueResult lockRead(Slice s, int version);

    ValueResult unlockRead(Slice s, int version);

    ValueResult lockWrite(Slice s, int version);

    ValueResult unlockWrite(Slice s);

    ValueResult deleteValue(Slice s, int version);

    ValueResult isValueDeleted(Slice s, int version);

    int getOffHeapVersion(Slice s);

    int NOVA_HEADER_SIZE = 4;
    int INVALID_VERSION = 0;

    void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts);

    <T> Result<T> transform(Slice s, Function<ByteBuffer, T> transformer, int version);

    <V> ValueResult put(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                        MemoryManager memoryManager);

    ValueResult compute(Slice s, Consumer<OakWBuffer> computer, int version);

    <V> Result<V> remove(Slice s, MemoryManager memoryManager, int version, V oldValue,
                         Function<ByteBuffer, V> transformer);

    <V> Result<V> exchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V value,
                           Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
                           MemoryManager memoryManager);

    <V> ValueResult compareExchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V expected, V value, Function<ByteBuffer,
            V> valueDeserializeTransformer, OakSerializer<V> serializer, MemoryManager memoryManager);
}
