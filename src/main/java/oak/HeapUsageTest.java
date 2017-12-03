package oak;

import java.nio.ByteBuffer;

public class HeapUsageTest {

    public static void main(String [] args) throws InterruptedException {
        final long K = 1024;
        final long M = K * K;

        int keySize = 10;
        int valSize = (int)Math.round(5*K);
        int numOfEntries = 360000;

        System.out.println("key size: " + keySize + "B" + ", value size: " + ((double)valSize)/K + "KB");

        ByteBuffer key = ByteBuffer.allocate(keySize);
        ByteBuffer val = ByteBuffer.allocate(valSize);
        key.putInt(0,0);
        val.putInt(0,0);

        OakMapOffHeapImpl oak = new OakMapOffHeapImpl();

        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        System.out.println("\nBefore filling up oak");
        System.out.println("heap size: " + heapSize/M + "MB" + ", heap max size: " + heapMaxSize/M + "MB" + ", heap free size: " + heapFreeSize/M + "MB");
        System.out.println("heap used: " + (heapSize - heapFreeSize)/M + "MB");
        System.out.println("off heap used: " + oak.memoryManager.pool.allocated()/M + "MB");


        for(int i = 0 ; i < numOfEntries ; i++){
            key.putInt(0,i);
            val.putInt(0,i);
            oak.put(key,val);
        }
        System.out.println("\nAfter filling up oak");
        System.out.println("off heap used: " + oak.memoryManager.pool.allocated()/M + "MB");

        System.gc();

        heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
        heapFreeSize = Runtime.getRuntime().freeMemory();
        System.out.println("heap size: " + heapSize/M + "MB" + ", heap max size: " + heapMaxSize/M + "MB" + ", heap free size: " + heapFreeSize/M + "MB");
        System.out.println("heap used: " + (heapSize - heapFreeSize)/M + "MB");

        for(int i = 0 ; i < numOfEntries ; i++){
            key.putInt(0,i);
            val.putInt(0,i);
            oak.put(key,val);
        }

        for(int i = 0 ; i < numOfEntries ; i++){
            key.putInt(0,i);
            OakBuffer buffer = oak.getHandle(key);
            if(buffer == null) {
                System.out.println("buffer != null i==" + i);
                return;
            }
            if(buffer.getInt(0) != i) {
                System.out.println("buffer.getInt(0) != i i==" + i);
                return;
            }
        }
        System.out.println("\nCheck again");
        System.out.println("off heap used: " + oak.memoryManager.pool.allocated()/M + "MB");
        System.out.println("off heap allocated: " + Integer.MAX_VALUE/M + "MB");
        System.gc();
        heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        heapMaxSize = Runtime.getRuntime().maxMemory(); // Get maximum size of heap in bytes
        heapFreeSize = Runtime.getRuntime().freeMemory();
        System.out.println("heap size: " + heapSize/M + "MB" + ", heap max size: " + heapMaxSize/M + "MB" + ", heap free size: " + heapFreeSize/M + "MB");
        System.out.println("heap used: " + (heapSize - heapFreeSize)/M + "MB");
        float percent = (100*(heapSize - heapFreeSize))/oak.memoryManager.pool.allocated();
        System.out.println("\non/off heap used: " + String.format("%.0f%%",percent));

    }
}
