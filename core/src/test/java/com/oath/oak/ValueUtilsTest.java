package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static com.oath.oak.ValueUtils.ValueResult.*;
import static org.junit.Assert.*;

public class ValueUtilsTest {
    private NovaManager novaManager;
    private Slice s;
    private final ValueUtils valueOperator = new ValueUtilsImpl();

    @Before
    public void init() {
        novaManager = new NovaManager(new OakNativeMemoryAllocator(128));
        s = novaManager.allocateSlice(20, MemoryManager.Allocate.VALUE);
        putInt(0, 1);
        valueOperator.initHeader(s);
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

        Result<Integer> result = valueOperator.transform(s,
                byteBuffer -> byteBuffer.getInt(0) + byteBuffer.getInt(4) + byteBuffer.getInt(8), 1);
        assertEquals(TRUE, result.operationResult);
        assertEquals(60, result.value.intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformUpperBoundTest() {
        valueOperator.transform(s, byteBuffer -> byteBuffer.getInt(12), 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformLowerBoundTest() {
        valueOperator.transform(s, byteBuffer -> byteBuffer.getInt(-4), 1);
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
            Result<Integer> result = valueOperator.transform(s, byteBuffer -> byteBuffer.getInt(4), 1);
            assertEquals(TRUE, result.operationResult);
            assertEquals(randomValue, result.value.intValue());
        });
        assertEquals(TRUE, valueOperator.lockWrite(s, 1));
        transformer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(12, randomValue);
        valueOperator.unlockWrite(s);
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
                Result<Integer> result = valueOperator.transform(s, byteBuffer -> byteBuffer.getInt(index), 1);
                assertEquals(TRUE, result.operationResult);
                assertEquals(10 + index, result.value.intValue());
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
        valueOperator.deleteValue(s, 1);
        Result<Integer> result = valueOperator.transform(s, byteBuffer -> byteBuffer.getInt(0), 1);
        assertEquals(FALSE, result.operationResult);
    }

    @Test
    public void cannotTransformedDifferentVersionTest() {
        Result<Integer> result = valueOperator.transform(s, byteBuffer -> byteBuffer.getInt(0), 2);
        assertEquals(RETRY, result.operationResult);
    }

    @Test
    public void putWithNoResizeTest() {
        EntrySet.LookUp lookUp = new EntrySet.LookUp(
            s, 0, 0, 1, EntrySet.INVALID_KEY_REFERENCE, false);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        assertEquals(TRUE, valueOperator.put(null, lookUp, 10, new OakSerializer<Integer>() {
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
        }, novaManager, null));
        assertEquals(randomValues[0], getInt(8));
        assertEquals(randomValues[1], getInt(12));
        assertEquals(randomValues[2], getInt(16));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putUpperBoundTest() {
        EntrySet.LookUp lookUp = new EntrySet.LookUp(
            s, 0, 0, 1, EntrySet.INVALID_KEY_REFERENCE, false);
        valueOperator.put(null, lookUp, 5, new OakSerializer<Integer>() {
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
        }, novaManager, null);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putLowerBoundTest() {
        EntrySet.LookUp lookUp = new EntrySet.LookUp(
            s, 0, 0, 1, EntrySet.INVALID_KEY_REFERENCE, false);
        valueOperator.put(null, lookUp, 5, new OakSerializer<Integer>() {
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
        }, novaManager, null);
    }

    @Test
    public void cannotPutReadLockedTest() throws InterruptedException {
        EntrySet.LookUp lookUp = new
            EntrySet.LookUp(s, 0, 0, 1, EntrySet.INVALID_KEY_REFERENCE, false);
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
            valueOperator.put(null, lookUp, 10, new OakSerializer<Integer>() {
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
            }, novaManager, null);
        });
        valueOperator.lockRead(s, 1);
        putter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int a = getInt(8), b = getInt(12), c = getInt(16);
        valueOperator.unlockRead(s, 1);
        putter.join();
        assertNotEquals(randomValues[0], a);
        assertNotEquals(randomValues[1], b);
        assertNotEquals(randomValues[2], c);
    }

    @Test
    public void cannotPutWriteLockedTest() throws InterruptedException {
        EntrySet.LookUp lookUp =
            new EntrySet.LookUp(s, 0, 0, 1, EntrySet.INVALID_KEY_REFERENCE, false);
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
            valueOperator.put(null, lookUp, 10, new OakSerializer<Integer>() {
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
            }, novaManager, null);
        });
        valueOperator.lockWrite(s, 1);
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
        valueOperator.unlockWrite(s);
        putter.join();
    }

    @Test
    public void cannotPutInDeletedValueTest() {
        valueOperator.deleteValue(s, 1);
        EntrySet.LookUp lookUp = new EntrySet.LookUp(s, 0, 0, 1, EntrySet.INVALID_KEY_REFERENCE,
            false);
        assertEquals(FALSE, valueOperator.put(null, lookUp, null, null, novaManager, null));
    }

    @Test
    public void cannotPutToValueOfDifferentVersionTest() {
        EntrySet.LookUp lookUp = new EntrySet.LookUp(s, 0, 0, 2, EntrySet.INVALID_KEY_REFERENCE,
            false);
        assertEquals(RETRY, valueOperator.put(null, lookUp, null, null, novaManager, null));
    }

    @Test
    public void computeTest() {
        int value = new Random().nextInt(128);
        putInt(8, value);
        valueOperator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(0, oakWBuffer.getInt(0) * 2);
        }, 1);
        assertEquals(value * 2, getInt(8));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeUpperBoundTest() {
        valueOperator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(12, 10);
        }, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeLowerBoundTest() {
        valueOperator.compute(s, oakWBuffer -> {
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
            valueOperator.compute(s, oakWBuffer -> {
                for (int i = 0; i < 12; i += 4) {
                    oakWBuffer.putInt(i, oakWBuffer.getInt(i) + 1);
                }
            }, 1);
        });
        valueOperator.lockRead(s, 1);
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
        valueOperator.unlockRead(s, 1);
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
            valueOperator.compute(s, oakWBuffer -> {
                for (int i = 0; i < 12; i += 4) {
                    oakWBuffer.putInt(i, oakWBuffer.getInt(i) + 1);
                }
            }, 1);
        });
        valueOperator.lockWrite(s, 1);
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
        valueOperator.unlockWrite(s);
        computer.join();
        assertNotEquals(randomValues[0], getInt(8));
        assertNotEquals(randomValues[1], getInt(12));
        assertNotEquals(randomValues[2], getInt(16));
    }

    @Test
    public void cannotComputeDeletedValueTest() {
        valueOperator.deleteValue(s, 1);
        assertEquals(FALSE, valueOperator.compute(s, oakWBuffer -> {
        }, 1));
    }

    @Test
    public void cannotComputeValueOfDifferentVersionTest() {
        assertEquals(RETRY, valueOperator.compute(s, oakWBuffer -> {
        }, 2));
    }
}
