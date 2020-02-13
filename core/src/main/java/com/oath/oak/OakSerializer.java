/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;

/**
 * An interface to be supported by keys and values provided for Oak's mapping
 */
public interface OakSerializer<T> {

  // serializes the object
  void serialize(T object, ByteBuffer targetBuffer);

  // deserializes the given byte buffer
  T deserialize(ByteBuffer byteBuffer);

  // deserializes the given byte buffer
  default T deserializePool(ByteBuffer byteBuffer, T pooledObj) {
    return null;
  }

  // returns the number of bytes needed for serializing the given object
  int calculateSize(T object);

  // hash function from serialized version of the object to an integer
  default int serializedHash(ByteBuffer object) {
    return -1;
  }

  // hash function from a key to an integer
  default int hash(T object) {
    return -1;
  }
}
