/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;

public interface ValueSerializer<K, V> {

  // serializes the value (may use the key)
  void serialize(K key, V value, ByteBuffer targetBuffer);

  // deserializes the given byte buffer
  V deserialize(ByteBuffer serializedKey, ByteBuffer serializedVlue);
}
