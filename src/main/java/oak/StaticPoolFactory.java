/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

public class StaticPoolFactory {
  private static MemoryPool memoryPool;

  public static MemoryPool getPool() {
      if(memoryPool == null) {
        memoryPool = new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE);
      }

      return memoryPool;
  }
}
