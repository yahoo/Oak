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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;



@RunWith(Parameterized.class)
public class NativeManagerTest {

    private Supplier<MemoryManager> supplier;
    private MemoryManager memoryManager;

    @Parameterized.Parameters
    public static Collection parameters() {

        Supplier<SyncRecycleMemoryManager> s1 = () -> {
            final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
            return new SyncRecycleMemoryManager(allocator);
        };

        Supplier<NovaMemoryManager> s2 = () -> {
            final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
            return new NovaMemoryManager(allocator);
        };

        return Arrays.asList(new Object[][] {
                { s1 },
                { s2 }
        });
    }

    public NativeManagerTest(Supplier<MemoryManager> supplier) {
        this.supplier = supplier;

    }
    
    @Before
    public void setUp() {
        memoryManager = supplier.get();
    }
    
    @After
    public void tearDown() {
        BlocksPool.clear();
    }

    @Test
    public void reuseTest() {
        long oldVersion = 0;
        if (memoryManager instanceof NovaMemoryManager) {
            oldVersion = ((NovaMemoryManager) memoryManager).getCurrentVersion();
        } else {
            oldVersion = ((SyncRecycleMemoryManager) memoryManager).getCurrentVersion();
        }
        BlockAllocationSlice[] allocatedSlices = new BlockAllocationSlice[memoryManager.getReleaseLimit()];
        for (int i = 0; i < memoryManager.getReleaseLimit(); i++) {
            allocatedSlices[i] = (BlockAllocationSlice) memoryManager.getEmptySlice();
            allocatedSlices[i].allocate(i + 5, false);
        }
        for (int i = 0; i < memoryManager.getReleaseLimit(); i++) {
            Assert.assertEquals(i + 5, allocatedSlices[i].getLength());
            allocatedSlices[i].release();
        }
        Assert.assertEquals(memoryManager.getReleaseLimit(), memoryManager.getFreeListSize());
        long newVersion = 0;
        if (memoryManager instanceof NovaMemoryManager) {
            newVersion = ((NovaMemoryManager) memoryManager).getCurrentVersion();
        } else {
            newVersion = ((SyncRecycleMemoryManager) memoryManager).getCurrentVersion();
        }
        Assert.assertEquals(oldVersion + 1, newVersion);
        for (int i = memoryManager.getReleaseLimit() - 1; i > -1; i--) {
            BlockAllocationSlice s = (BlockAllocationSlice) memoryManager.getEmptySlice();
            s.allocate(i + 5, false);
            Assert.assertEquals(allocatedSlices[i].getAllocatedBlockID(), s.getAllocatedBlockID());
            Assert.assertEquals(allocatedSlices[i].getAllocatedLength(), s.getAllocatedLength());
            Assert.assertEquals(allocatedSlices[i].getAllocatedOffset(), s.getAllocatedOffset());
        }
    }
}
