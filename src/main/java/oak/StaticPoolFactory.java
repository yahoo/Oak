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
