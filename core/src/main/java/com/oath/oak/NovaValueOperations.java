package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface NovaValueOperations extends NovaValueUtils {

    void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts);

    <T> Result<T> transform(Slice s, Function<ByteBuffer, T> transformer, int version);

    <V> NovaResult put(Chunk<?, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer,
                          NovaManager memoryManager);

    NovaResult compute(Slice s, Consumer<OakWBuffer> computer, int version);

    <V> Result<V> remove(Slice s, NovaManager memoryManager, int version, V oldValue,
                         Function<ByteBuffer, V> transformer);

    <V> Result<V> exchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V value,
                           Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer,
                           NovaManager memoryManager);

    <V> NovaResult compareExchange(Chunk<?, V> chunk, Chunk.LookUp lookUp, V expected, V value, Function<ByteBuffer,
            V> valueDeserializeTransformer, OakSerializer<V> serializer, NovaManager memoryManager);
}
