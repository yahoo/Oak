package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.NovaValueUtils.Result.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NovaValueUtilsTest {
    private NovaManager novaManager;
    private Slice s;
    private final NovaValueUtils operator = new NovaValueOperationsImpl();

    @Before
    public void init() {
        novaManager = new NovaManager(new OakNativeMemoryAllocator(128));
        s = novaManager.allocateSlice(16, false);
        s.getByteBuffer().putInt(s.getByteBuffer().position(), 1);
    }

    @Test
    public void testCannotReadLockDeleted() {
        assertEquals(TRUE, operator.deleteValue(s, 1));
        assertEquals(FALSE, operator.lockRead(s, 1));
    }

    @Test
    public void testCannotWriteLockDeleted() {
        assertEquals(TRUE, operator.deleteValue(s, 1));
        assertEquals(FALSE, operator.lockWrite(s, 1));
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        assertEquals(TRUE, operator.deleteValue(s, 1));
        assertEquals(FALSE, operator.deleteValue(s, 1));
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        for (int i = 0; i < 10000; i++) {
            assertEquals(TRUE, operator.lockRead(s, 1));
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
            assertEquals(TRUE, operator.lockWrite(s, 1));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.lockRead(s, 1));
            flag.incrementAndGet();
            operator.unlockRead(s, 1);
        });
        assertEquals(TRUE, operator.lockRead(s, 1));
        writer.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        operator.unlockRead(s, 1);
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
            assertEquals(TRUE, operator.deleteValue(s, 1));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.lockRead(s, 1));
            flag.incrementAndGet();
            operator.unlockRead(s, 1);
        });
        assertEquals(TRUE, operator.lockRead(s, 1));
        deleter.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        operator.unlockRead(s, 1);
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
            assertEquals(TRUE, operator.lockRead(s, 1));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, operator.lockWrite(s, 1));
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        operator.unlockWrite(s);
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
            assertEquals(TRUE, operator.lockWrite(s, 1));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, operator.lockWrite(s, 1));
        writer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        operator.unlockWrite(s);
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
            assertEquals(TRUE, operator.deleteValue(s, 1));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, operator.lockWrite(s, 1));
        deleter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        operator.unlockWrite(s);
        deleter.join();
    }

    @Test
    public void testCannotReadLockDifferentVersion() {
        assertEquals(RETRY, operator.lockRead(s, 2));
    }

    @Test
    public void testCannotWriteLockDifferentVersion() {
        assertEquals(RETRY, operator.lockWrite(s, 2));
    }

    @Test
    public void testCannotDeletedDifferentVersion() {
        assertEquals(RETRY, operator.deleteValue(s, 2));
    }
}
