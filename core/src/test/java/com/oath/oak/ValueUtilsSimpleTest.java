package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.ValueUtils.ValueResult.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ValueUtilsSimpleTest {
    private Slice s;
    private final ValueUtils valueOperator = new ValueUtilsImpl();

    @Before
    public void init() {
        NovaManager novaManager = new NovaManager(new OakNativeMemoryAllocator(128));
        s = novaManager.allocateSlice(16, MemoryManager.Allocate.VALUE);
        s.getByteBuffer().putInt(s.getByteBuffer().position(), 1);
        valueOperator.initHeader(s);
    }

    @Test
    public void testCannotReadLockDeleted() {
        assertEquals(TRUE, valueOperator.deleteValue(s, 1));
        assertEquals(FALSE, valueOperator.lockRead(s, 1));
    }

    @Test
    public void testCannotWriteLockDeleted() {
        assertEquals(TRUE, valueOperator.deleteValue(s, 1));
        assertEquals(FALSE, valueOperator.lockWrite(s, 1));
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        assertEquals(TRUE, valueOperator.deleteValue(s, 1));
        assertEquals(FALSE, valueOperator.deleteValue(s, 1));
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        for (int i = 0; i < 10000; i++) {
            assertEquals(TRUE, valueOperator.lockRead(s, 1));
        }
    }

    @Test
    public void testCannotWriteLockReadLocked() throws InterruptedException {
        AtomicInteger flag = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(3);
        Thread writer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, valueOperator.lockWrite(s, 1));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, valueOperator.lockRead(s, 1));
            flag.incrementAndGet();
            valueOperator.unlockRead(s, 1);
        });
        assertEquals(TRUE, valueOperator.lockRead(s, 1));
        writer.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        valueOperator.unlockRead(s, 1);
        reader.join();
        writer.join();
    }

    @Test
    public void testCannotDeletedReadLocked() throws InterruptedException {
        AtomicInteger flag = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(3);
        Thread deleter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, valueOperator.deleteValue(s, 1));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, valueOperator.lockRead(s, 1));
            flag.incrementAndGet();
            valueOperator.unlockRead(s, 1);
        });
        assertEquals(TRUE, valueOperator.lockRead(s, 1));
        deleter.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        valueOperator.unlockRead(s, 1);
        reader.join();
        deleter.join();
    }

    @Test
    public void testCannotReadLockWriteLocked() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, valueOperator.lockRead(s, 1));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, valueOperator.lockWrite(s, 1));
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        valueOperator.unlockWrite(s);
        reader.join();
    }

    @Test
    public void testCannotWriteLockMultipleTimes() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread writer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, valueOperator.lockWrite(s, 1));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, valueOperator.lockWrite(s, 1));
        writer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        valueOperator.unlockWrite(s);
        writer.join();
    }

    @Test
    public void testCannotDeletedWriteLocked() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread deleter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, valueOperator.deleteValue(s, 1));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, valueOperator.lockWrite(s, 1));
        deleter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        valueOperator.unlockWrite(s);
        deleter.join();
    }

    @Test
    public void testCannotReadLockDifferentVersion() {
        assertEquals(RETRY, valueOperator.lockRead(s, 2));
    }

    @Test
    public void testCannotWriteLockDifferentVersion() {
        assertEquals(RETRY, valueOperator.lockWrite(s, 2));
    }

    @Test
    public void testCannotDeletedDifferentVersion() {
        assertEquals(RETRY, valueOperator.deleteValue(s, 2));
    }
}
