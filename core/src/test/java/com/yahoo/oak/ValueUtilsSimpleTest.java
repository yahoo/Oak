/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ValueUtilsSimpleTest {
    private AbstractSlice s;

    @Before
    public void init() {
        SyncRecycleMemoryManager syncRecycleMemoryManager =
            new SyncRecycleMemoryManager(new NativeMemoryAllocator(128));
        s = syncRecycleMemoryManager.getEmptySlice();
        s.allocate(16, false);
        UnsafeUtils.UNSAFE.putInt(s.getAddress(), 1);
    }

    @Test
    public void testCannotReadLockDeleted() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, s.lockRead());
    }

    @Test
    public void testCannotWriteLockDeleted() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, s.lockWrite());
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, s.lockWrite());
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockRead());
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockWrite());
            Assert.assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockRead());
            flag.incrementAndGet();
            s.unlockRead();
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockRead());
        writer.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        s.unlockRead();
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
            Assert.assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockRead());
            flag.incrementAndGet();
            s.unlockRead();
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockRead());
        deleter.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        s.unlockRead();
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockRead());
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockWrite());
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        s.unlockWrite();
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockWrite());
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockWrite());
        writer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        s.unlockWrite();
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.lockWrite());
        deleter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        s.unlockWrite();
        deleter.join();
    }

    @Test
    public void testCannotReadLockDifferentVersion() {
        s.associateMMAllocation(2, -1);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, s.lockRead());
    }

    @Test
    public void testCannotWriteLockDifferentVersion() {
        s.associateMMAllocation(2, -1);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, s.lockWrite());
    }

    @Test
    public void testCannotDeletedDifferentVersion() {
        s.associateMMAllocation(2, -1);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, s.logicalDelete());
    }
}
