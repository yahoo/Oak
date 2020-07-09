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
    private Slice s;
    private final ValueUtils valueOperator = new ValueUtilsImpl();

    @Before
    public void init() {
        NovaManager novaManager = new NovaManager(new NativeMemoryAllocator(128));
        s = new Slice();
        novaManager.allocate(s, 16, MemoryManager.Allocate.VALUE);
        s.buffer.putInt(s.getOffset(), 1);
        valueOperator.initHeader(s);
    }

    @Test
    public void testCannotReadLockDeleted() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.deleteValue(s));
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, valueOperator.lockRead(s));
    }

    @Test
    public void testCannotWriteLockDeleted() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.deleteValue(s));
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, valueOperator.lockWrite(s));
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.deleteValue(s));
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, valueOperator.deleteValue(s));
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockRead(s));
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockWrite(s));
            Assert.assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockRead(s));
            flag.incrementAndGet();
            valueOperator.unlockRead(s);
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockRead(s));
        writer.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        valueOperator.unlockRead(s);
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.deleteValue(s));
            Assert.assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockRead(s));
            flag.incrementAndGet();
            valueOperator.unlockRead(s);
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockRead(s));
        deleter.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        valueOperator.unlockRead(s);
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockRead(s));
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockWrite(s));
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockWrite(s));
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockWrite(s));
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
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.deleteValue(s));
            Assert.assertTrue(flag.get());
        });
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, valueOperator.lockWrite(s));
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
        s.setVersion(2);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, valueOperator.lockRead(s));
    }

    @Test
    public void testCannotWriteLockDifferentVersion() {
        s.setVersion(2);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, valueOperator.lockWrite(s));
    }

    @Test
    public void testCannotDeletedDifferentVersion() {
        s.setVersion(2);
        Assert.assertEquals(ValueUtils.ValueResult.RETRY, valueOperator.deleteValue(s));
    }
}
