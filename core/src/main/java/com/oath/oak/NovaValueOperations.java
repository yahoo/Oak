package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface NovaValueOperations extends NovaValueUtils {

    int NOVA_HEADER_SIZE = 4;
    int INVALID_VERSION = 0;

    void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts);

    <T> Result<T> transform(Slice s, Function<ByteBuffer, T> transformer, int version);

    <V> NovaResult put(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                       MemoryManager memoryManager);

    NovaResult compute(Slice s, Consumer<OakWBuffer> computer, int version);

    <V> Result<V> remove(Slice s, MemoryManager memoryManager, int version, V oldValue,
                         Function<ByteBuffer, V> transformer);

    <V> Result<V> exchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V value,
                           Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
                           MemoryManager memoryManager);

    <V> NovaResult compareExchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V expected, V value, Function<ByteBuffer,
            V> valueDeserializeTransformer, OakSerializer<V> serializer, MemoryManager memoryManager);
}
