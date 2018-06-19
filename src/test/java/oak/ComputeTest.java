package oak;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class ComputeTest {

    private static int NUM_THREADS;

    static OakMap oak;
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

    static private Consumer<WritableOakBuffer> func = buffer -> {
        if (buffer.getInt(0) == buffer.getInt(40)) {
            return;
        }
        int[] arr = new int[10];
        for (int i = 0; i < 50; i++) {
            for (int j = 0; j < 10; j++) {
                arr[j] = buffer.getInt();
            }
            for (int j = 0; j < 10; j++) {
                buffer.putInt(arr[j]);
            }
        }
    };

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

            ByteBuffer myKey = ByteBuffer.allocate(keySize);
            ByteBuffer myVal = ByteBuffer.allocate(valSize);

            Random r = new Random();

            for (int i = 0; i < 1000000; i++) {
                int k = r.nextInt(numOfEntries);
                int o = r.nextInt(2);
                myKey.putInt(0, k);
                myVal.putInt(0, k);
                if (o % 2 == 0)
                    oak.computeIfPresent(myKey, func);
                else
                    oak.putIfAbsent(myKey, myVal);

            }

        }
    }

    public static void main(String[] args) throws InterruptedException {

        int maxItemsPerChunk = 2048;
        int maxBytesPerChunkItem = 100;

        oak = new OakMapOffHeapImpl(maxItemsPerChunk, maxBytesPerChunkItem);

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
            OakBuffer buffer = oak.get(key);
            if (buffer == null) {
                continue;
            }
            if (buffer.getInt(0) != i) {
                System.out.println("buffer.getInt(0) != i i==" + i);
                return;
            }
            int forty = buffer.getInt(40);
            if (forty != i && forty != 0) {
                System.out.println(buffer.getInt(40));
                return;
            }
        }

        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);

    }
}
