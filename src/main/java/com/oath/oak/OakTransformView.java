/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.function.Function;
import java.util.Map;

/**
 * This class allows data retrieval from OakMap while the output is set as transformed mappings,
 * using a user-supplied transform function
 *
 * @param <K> The key object type.
 * @param <T> The transformation object type
 */

public class OakTransformView<K, T> implements AutoCloseable{

  private final InternalOakMap internalOakMap;  // to hold the actual data
  private final OakMap externalOakMap;          // to hold the boundaries and memory manager
  private final K fromKey;
  private final K toKey;
  private final Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;
  private final Function<ByteBuffer, T> valuesTransformer;
  private final Function<ByteBuffer, T> keysTransformer;

  // package level view constructor, to be executed by OakMap only
  OakTransformView(InternalOakMap iOM, OakMap eOM, K fromKey, K toKey,
                   Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
    this.internalOakMap = iOM;
    internalOakMap.open();
    this.externalOakMap = eOM;
    this.fromKey = fromKey;
    this.toKey = toKey;
    this.transformer = transformer;
    this.valuesTransformer = new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer byteBuffer) {
        return transformer.apply(new AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer>(null, byteBuffer));
      }
    };
    this.keysTransformer = new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer byteBuffer) {
        return transformer.apply(new AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer>(byteBuffer, null));
      }
    };
  }

  public T get(K key) {
    if (key == null)
      throw new NullPointerException();
    if (!externalOakMap.inBounds(key))
      throw new IllegalArgumentException();
    try {
      externalOakMap.getMemoryManager().startOperation();
      return (T) internalOakMap.getValueTransformation(key, valuesTransformer);
    } finally {
      externalOakMap.getMemoryManager().stopOperation();
    }
  }

  /**
   * Returns a {@link OakCloseableIterator} of a transformation of values contained in this map
   * in ascending order of the corresponding keys.
   */
  public OakCloseableIterator<T> valuesIterator() {
    return internalOakMap.valuesTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), valuesTransformer);
  }

  /**
   * Returns a {@link OakCloseableIterator} of a transformation of the mappings contained in this map in ascending key order.
   */
  public OakCloseableIterator<T> entriesIterator() {
    return internalOakMap.entriesTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), transformer);
  }

  /**
   * Returns a {@link OakCloseableIterator} of a transformation of the keys contained in this map in ascending order.
   */
  public OakCloseableIterator<T> keysIterator() {
    return internalOakMap.keysTransformIterator(
            fromKey, externalOakMap.getFromInclusive(),
            toKey, externalOakMap.getToInclusive(),
            externalOakMap.getIsDescending(), keysTransformer);
  }

  /**
   * Closes this resource, relinquishing any underlying resources.
   * This method is invoked automatically on objects managed by the
   * {@code try}-with-resources statement.
   * <p>While this interface method is declared to throw {@code
   * Exception}, implementers are <em>strongly</em> encouraged to
   * declare concrete implementations of the {@code close} method to
   * throw more specific exceptions, or to throw no exception at all
   * if the close operation cannot fail.
   * <p> Cases where the close operation may fail require careful
   * attention by implementers. It is strongly advised to relinquish
   * the underlying resources and to internally <em>mark</em> the
   * resource as closed, prior to throwing the exception. The {@code
   * close} method is unlikely to be invoked more than once and so
   * this ensures that the resources are released in a timely manner.
   * Furthermore it reduces problems that could arise when the resource
   * wraps, or is wrapped, by another resource.
   * <p><em>Implementers of this interface are also strongly advised
   * to not have the {@code close} method throw {@link
   * InterruptedException}.</em>
   * This exception interacts with a thread's interrupted status,
   * and runtime misbehavior is likely to occur if an {@code
   * InterruptedException} is {@linkplain Throwable#addSuppressed
   * suppressed}.
   * More generally, if it would cause problems for an
   * exception to be suppressed, the {@code AutoCloseable.close}
   * method should not throw it.
   * <p>Note that unlike the {@link Closeable#close close}
   * method of {@link Closeable}, this {@code close} method
   * is <em>not</em> required to be idempotent.  In other words,
   * calling this {@code close} method more than once may have some
   * visible side effect, unlike {@code Closeable.close} which is
   * required to have no effect if called more than once.
   * However, implementers of this interface are strongly encouraged
   * to make their {@code close} methods idempotent.
   *
   * @throws Exception if this resource cannot be closed
   */
  @Override public void close() throws Exception {
    internalOakMap.close();
  }
}
