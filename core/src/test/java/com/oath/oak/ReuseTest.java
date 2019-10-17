package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore
public class ReuseTest {

    private OakMap<String, String> oak;

    private String generateString(int value, int length) {
        StringBuilder builder = new StringBuilder();
        builder.append(value);
        int remaining = length - builder.length();
        for (int i = 0; i < remaining; i++) {
            builder.append("-");
        }
        return builder.toString();
    }

    @Before
    public void init() {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("")
                .setChunkMaxItems(1024);
        oak = builder.build();
    }

    private void printHeapStats(String message) {
        System.gc();
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        System.out.println("\n" + message);
        System.out.println((float) (heapSize - heapFreeSize) / (1024 * 1024));
        final OakNativeMemoryAllocator memoryAllocator = oak.getMemoryManager().getAllocator();
        System.out.println((float) (memoryAllocator.allocated()) / (1024 * 1024));
        System.out.println("Free List Length " + memoryAllocator.getFreeListLength());
        System.out.println("Keys " + memoryAllocator.keysAllocated.get());
        System.out.println("Values " + memoryAllocator.valuesAllocated.get());
    }

    @Test
    public void main() throws InterruptedException {
        int numOfEntries = 2_000_000;
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger numPuts = new AtomicInteger(), numRemoves = new AtomicInteger();
        Thread worker = new Thread(() -> {
            Random random = new Random();
            while (!stop.get()) {
                int key = random.nextInt(numOfEntries);
                boolean op = random.nextInt(1000) < 500;
                if (op) {
                    oak.zc().put(generateString(key, 50), generateString(key, 500));
                    numPuts.getAndIncrement();
                } else {
                    oak.zc().remove(generateString(key, 50));
                    numRemoves.getAndIncrement();
                }
            }
        });

        printHeapStats("Before Init");

        Random random = new Random();
        for (int i = 0; i < numOfEntries / 2; ) {
            final int key = random.nextInt(numOfEntries);
            if (oak.zc().putIfAbsent(generateString(key, 50), generateString(key, 500))) {
                i++;
            }
        }

        printHeapStats("After Init");

        worker.start();

        for (int i = 0; i < 10; i++) {
            Thread.sleep(5000);
            printHeapStats("Sample " + (i + 1));
            System.out.println("Puts " + numPuts.get());
            System.out.println("Removes " + numRemoves.get());
        }
        stop.set(true);
        worker.join();
    }
}
