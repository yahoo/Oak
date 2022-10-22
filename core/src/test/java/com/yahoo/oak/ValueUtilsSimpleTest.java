/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ValueUtilsSimpleTest {
    SyncRecycleMemoryManager syncRecycleMemoryManager;
    private BlockAllocationSlice s;

    @Before
    public void init() {
        syncRecycleMemoryManager =
            new SyncRecycleMemoryManager(new NativeMemoryAllocator(128));
        s = syncRecycleMemoryManager.getEmptySlice();
        s.allocate(16, false);
        DirectUtils.putInt(s.getAddress(), 1);
    }

    @After
    public void tearDown() {
        syncRecycleMemoryManager.close();
        BlocksPool.clear();
    }

    @Test
    public void testCannotReadLockDeleted() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, s.preRead());
    }

    @Test
    public void testCannotWriteLockDeleted() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, s.preWrite());
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.logicalDelete());
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, s.preWrite());
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preRead());
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preWrite());
            Assert.assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preRead());
            flag.incrementAndGet();
            try {
                s.postRead();
            } catch (DeletedMemoryAccessException e) {
                e.printStackTrace();
            }
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preRead());
        writer.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        try {
            s.postRead();
        } catch (DeletedMemoryAccessException e) {
            e.printStackTrace();
        }
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preRead());
            flag.incrementAndGet();
            try {
                s.postRead();
            } catch (DeletedMemoryAccessException e) {
                e.printStackTrace();
            }
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preRead());
        deleter.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        try {
            s.postRead();
        } catch (DeletedMemoryAccessException e) {
            e.printStackTrace();
        }
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preRead());
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preWrite());
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        s.postWrite();
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preWrite());
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preWrite());
        writer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        s.postWrite();
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
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, s.preWrite());
        deleter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        s.postWrite();
        deleter.join();
    }

    @Test
    public void testCannotReadLockDifferentVersion() {
        s.associateMMAllocation(2, -1);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, s.preRead());
    }

    @Test
    public void testCannotWriteLockDifferentVersion() {
        s.associateMMAllocation(2, -1);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, s.preWrite());
    }

    @Test
    public void testCannotDeletedDifferentVersion() {
        s.associateMMAllocation(2, -1);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, s.logicalDelete());
    }
}
