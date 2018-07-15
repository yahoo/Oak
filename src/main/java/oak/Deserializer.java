/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;

/**
 * @param <T> The object type.
 */
public interface Deserializer<T> {

  // deserializes bytebuffer to object
  T deserialize(ByteBuffer byteBuffer);
}