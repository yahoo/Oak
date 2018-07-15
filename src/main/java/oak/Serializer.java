/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;

public interface Serializer<T> {

  // serializes object to byte buffer
  void serialize(T obj, ByteBuffer targetBuffer);
}
