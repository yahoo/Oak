package oak;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;

public class FillTest {

    private static int NUM_THREADS;

    static OakMap oak;
    static ConcurrentSkipListMap<ByteBuffer, ByteBuffer> skiplist;
    static boolean java = false;
    private static final long K = 1024;
//    private static final long M = K * K;
//    static final long G = M * K;

    private static int keySize = 10;
    private static int valSize = (int) Math.round(5 * K);
    private static int numOfEntries;

    static ByteBuffer key = ByteBuffer.allocate(keySize);
    private static ByteBuffer val = ByteBuffer.allocate(valSize);

    static private ArrayList<Thread> threads = new ArrayList<>(NUM_THREADS);
    static private CountDownLatch latch = new CountDownLatch(1);

    static class RunThreads implements Runnable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Random r = new Random();

            ByteBuffer myKey = ByteBuffer.allocate(keySize);
            ByteBuffer myVal = ByteBuffer.allocate(valSize);

            int id = Chunk.getIndex();
            int amount = (int) Math.round(numOfEntries * 0.5) / NUM_THREADS;
            int start = id * amount + (int) Math.round(numOfEntries * 0.5);
            int end = (id + 1) * amount + (int) Math.round(numOfEntries * 0.5);

            int[] arr = new int[amount];
            for (int i = start, j = 0; i < end; i++,j++) {
                arr[j] = i;
            }

            int usedIdx = arr.length-1;

            for (int i = 0; i < amount; i++) {

                int nextIdx = r.nextInt(usedIdx + 1);
                int next = arr[nextIdx];

                int tmp = arr[usedIdx];
                arr[usedIdx] = next;
                arr[nextIdx] = tmp;
                usedIdx--;

                if (java) {
                    ByteBuffer newKey = ByteBuffer.allocate(keySize);
                    for(int j = 0; j < keySize ; j++){
                        newKey.put(j,key.get(j));
                    }
                    ByteBuffer newVal = ByteBuffer.allocate(valSize);
                    for(int j = 0; j < valSize ; j++){
                        newVal.put(j,val.get(j));
                    }
                    newKey.putInt(0, next);
                    newVal.putInt(0, next);
                    skiplist.putIfAbsent(newKey, newVal);
                    continue;
                }
                myKey.putInt(0, next);
                myVal.putInt(0, next);
                oak.putIfAbsent(myKey, myVal);
            }

            for (int i = end-1; i >= start; i--) {
                myKey.putInt(0, i);
                if(java){
                    if(skiplist.get(myKey) == null){
                        System.out.println("error");
                    }
                    continue;
                }
                if(oak.get(myKey) == null){
                    System.out.println("error");
                }
            }

        }
    }

    public static void main(String[] args) throws InterruptedException {

        int maxItemsPerChunk = 2048;
        int maxBytesPerChunkItem = 100;

        if (args[0].equals("on")) {
            oak = new OakMapOnHeapImpl(maxItemsPerChunk, maxBytesPerChunkItem);
        } else if (args[0].equals("off")) {
            oak = new OakMapOffHeapImpl(maxItemsPerChunk, maxBytesPerChunkItem);
        } else if (args[0].equals("java")) {
            java = true;
            skiplist = new ConcurrentSkipListMap<>();
        }

        NUM_THREADS = Integer.parseInt(args[1]);

        numOfEntries = Integer.parseInt(args[2]);


//        System.out.println("Time to connect jconsole");
//        TimeUnit.SECONDS.sleep(20);

        key.putInt(0, 0);
        val.putInt(0, 0);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new Thread(new RunThreads(latch)));
        }

        for (int i = 0; i < (int) Math.round(numOfEntries * 0.5); i++) {
            if (java) {
                ByteBuffer newKey = ByteBuffer.allocate(keySize);
                ByteBuffer newVal = ByteBuffer.allocate(valSize);
                newKey.putInt(0, i);
                newVal.putInt(0, i);
                skiplist.putIfAbsent(newKey, newVal);
                continue;
            }
            key.putInt(0, i);
            val.putInt(0, i);
            oak.putIfAbsent(key, val);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }

        long startTime = System.currentTimeMillis();

        latch.countDown();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        long stopTime = System.currentTimeMillis();

        for (int i = 0; i < numOfEntries; i++) {
            key.putInt(0, i);
            if (java) {
                ByteBuffer bb = skiplist.get(key);
                if (bb == null) {
                    return;
                }
                if (bb.getInt(0) != i) {
                    return;
                }
                continue;
            }
            OakBuffer buffer = oak.get(key);
            if (buffer == null) {
                System.out.println("buffer != null i==" + i);
                return;
            }
            if (buffer.getInt(0) != i) {
                System.out.println("buffer.getInt(0) != i i==" + i);
                return;
            }
        }

        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);

    }
}
