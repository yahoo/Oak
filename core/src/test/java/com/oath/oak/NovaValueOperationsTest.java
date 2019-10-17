package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static com.oath.oak.NovaValueUtils.Result.*;
import static org.junit.Assert.*;

public class NovaValueOperationsTest {
    private NovaManager novaManager;
    private Slice s;
    private final NovaValueOperations operator = new NovaValueOperationsImpl();

    @Before
    public void init() {
        novaManager = new NovaManager(new OakNativeMemoryAllocator(128));
        s = novaManager.allocateSlice(20, false);
        putInt(0, 1);
        putInt(operator.getLockLocation(), 0);
    }

    private void putInt(int index, int value) {
        s.getByteBuffer().putInt(s.getByteBuffer().position() + index, value);
    }

    private int getInt(int index) {
        return s.getByteBuffer().getInt(s.getByteBuffer().position() + index);
    }

    @Test
    public void transformTest() {
        putInt(8, 10);
        putInt(12, 20);
        putInt(16, 30);

        Map.Entry<NovaValueUtils.Result, Integer> result = operator.transform(s,
                byteBuffer -> byteBuffer.getInt(0) + byteBuffer.getInt(4) + byteBuffer.getInt(8), 1);
        assertEquals(TRUE, result.getKey());
        assertEquals(60, result.getValue().intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformUpperBoundTest() {
        operator.transform(s, byteBuffer -> byteBuffer.getInt(12), 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformLowerBoundTest() {
        operator.transform(s, byteBuffer -> byteBuffer.getInt(-4), 1);
    }

    @Test(timeout = 5000)
    public void cannotTransformWriteLockedTest() throws InterruptedException {
        Random random = new Random();
        final int randomValue = random.nextInt();
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread transformer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Map.Entry<NovaValueUtils.Result, Integer> result = operator.transform(s,
                    byteBuffer -> byteBuffer.getInt(4), 1);
            assertEquals(TRUE, result.getKey());
            assertEquals(randomValue, result.getValue().intValue());
        });
        assertEquals(TRUE, operator.lockWrite(s, 1));
        transformer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(12, randomValue);
        operator.unlockWrite(s);
        transformer.join();
    }

    @Test
    public void multipleConcurrentTransformsTest() {
        putInt(8, 10);
        putInt(12, 14);
        putInt(16, 18);
        final int parties = 4;
        CyclicBarrier barrier = new CyclicBarrier(parties);
        Thread[] threads = new Thread[parties];
        for (int i = 0; i < parties; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                int index = new Random().nextInt(3) * 4;
                Map.Entry<NovaValueUtils.Result, Integer> result = operator.transform(s,
                        byteBuffer -> byteBuffer.getInt(index), 1);
                assertEquals(TRUE, result.getKey());
                assertEquals(10 + index, result.getValue().intValue());
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void cannotTransformDeletedTest() {
        operator.deleteValue(s, 1);
        Map.Entry<NovaValueUtils.Result, Integer> result = operator.transform(s, byteBuffer -> byteBuffer.getInt(0), 1);
        assertEquals(FALSE, result.getKey());
    }

    @Test
    public void cannotTransformedDifferentVersionTest() {
        Map.Entry<NovaValueUtils.Result, Integer> result = operator.transform(s, byteBuffer -> byteBuffer.getInt(0), 2);
        assertEquals(RETRY, result.getKey());
    }

    @Test
    public void putWithNoResizeTest() {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0, 1);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        assertEquals(TRUE, operator.put(null, lookUp, 10, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                for (int randomValue : randomValues) {
                    targetBuffer.putInt(randomValue);
                }
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, novaManager));
        assertEquals(randomValues[0], getInt(8));
        assertEquals(randomValues[1], getInt(12));
        assertEquals(randomValues[2], getInt(16));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putUpperBoundTest() {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0, 1);
        operator.put(null, lookUp, 5, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                targetBuffer.putInt(12, 30);
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, novaManager);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putLowerBoundTest() {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0, 1);
        operator.put(null, lookUp, 5, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                targetBuffer.putInt(-4, 30);
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, novaManager);
    }

    @Test
    public void cannotPutReadLockedTest() throws InterruptedException {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0, 1);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        Thread putter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.put(null, lookUp, 10, new OakSerializer<Integer>() {
                @Override
                public void serialize(Integer object, ByteBuffer targetBuffer) {
                    for (int randomValue : randomValues) {
                        targetBuffer.putInt(randomValue);
                    }
                }

                @Override
                public Integer deserialize(ByteBuffer byteBuffer) {
                    return null;
                }

                @Override
                public int calculateSize(Integer object) {
                    return 0;
                }
            }, novaManager);
        });
        operator.lockRead(s, 1);
        putter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int a = getInt(8), b = getInt(12), c = getInt(16);
        operator.unlockRead(s, 1);
        putter.join();
        assertNotEquals(randomValues[0], a);
        assertNotEquals(randomValues[1], b);
        assertNotEquals(randomValues[2], c);
    }

    @Test
    public void cannotPutWriteLockedTest() throws InterruptedException {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0, 1);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        putInt(8, randomValues[0] - 1);
        putInt(12, randomValues[1] - 1);
        putInt(16, randomValues[2] - 1);
        Thread putter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.put(null, lookUp, 10, new OakSerializer<Integer>() {
                @Override
                public void serialize(Integer object, ByteBuffer targetBuffer) {
                    for (int i = 0; i < targetBuffer.remaining(); i += 4) {
                        assertEquals(randomValues[i / 4], targetBuffer.getInt(i));
                    }
                }

                @Override
                public Integer deserialize(ByteBuffer byteBuffer) {
                    return null;
                }

                @Override
                public int calculateSize(Integer object) {
                    return 0;
                }
            }, novaManager);
        });
        operator.lockWrite(s, 1);
        putter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(8, randomValues[0]);
        putInt(12, randomValues[1]);
        putInt(16, randomValues[2]);
        operator.unlockWrite(s);
        putter.join();
    }

    @Test
    public void cannotPutInDeletedValueTest() {
        operator.deleteValue(s, 1);
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0, 1);
        assertEquals(FALSE, operator.put(null, lookUp, null, null, novaManager));
    }

    @Test
    public void cannotPutToValueOfDifferentVersionTest() {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0, 2);
        assertEquals(RETRY, operator.put(null, lookUp, null, null, novaManager));
    }

    @Test
    public void computeTest() {
        int value = new Random().nextInt(128);
        putInt(8, value);
        operator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(0, oakWBuffer.getInt(0) * 2);
        }, 1);
        assertEquals(value * 2, getInt(8));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeUpperBoundTest() {
        operator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(12, 10);
        }, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeLowerBoundTest() {
        operator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(-1, 10);
        }, 1);
    }

    @Test
    public void cannotComputeReadLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        putInt(8, randomValues[0]);
        putInt(12, randomValues[1]);
        putInt(16, randomValues[2]);
        Thread computer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.compute(s, oakWBuffer -> {
                for (int i = 0; i < 12; i += 4) {
                    oakWBuffer.putInt(i, oakWBuffer.getInt(i) + 1);
                }
            }, 1);
        });
        operator.lockRead(s, 1);
        computer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int[] results = new int[3];
        for (int i = 0; i < 3; i++) {
            results[i] = getInt(i * 4 + 8);
        }
        operator.unlockRead(s, 1);
        computer.join();
        assertArrayEquals(randomValues, results);
    }

    @Test
    public void cannotComputeWriteLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        putInt(8, randomValues[0] - 1);
        putInt(12, randomValues[1] - 1);
        putInt(16, randomValues[2] - 1);
        Thread computer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.compute(s, oakWBuffer -> {
                for (int i = 0; i < 12; i += 4) {
                    oakWBuffer.putInt(i, oakWBuffer.getInt(i) + 1);
                }
            }, 1);
        });
        operator.lockWrite(s, 1);
        computer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        for (int i = 8; i < 20; i += 4) {
            putInt(i, getInt(i) + 1);
        }
        operator.unlockWrite(s);
        computer.join();
        assertNotEquals(randomValues[0], getInt(8));
        assertNotEquals(randomValues[1], getInt(12));
        assertNotEquals(randomValues[2], getInt(16));
    }

    @Test
    public void cannotComputeDeletedValueTest() {
        operator.deleteValue(s, 1);
        assertEquals(FALSE, operator.compute(s, oakWBuffer -> {
        }, 1));
    }

    @Test
    public void cannotComputeValueOfDifferentVersionTest() {
        assertEquals(RETRY, operator.compute(s, oakWBuffer -> {
        }, 2));
    }
}
