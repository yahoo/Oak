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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class SanityMemoryManagerTest {

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
        
        Supplier<NovaMemoryManager> s3 = () -> {
            final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
            return new NovaMemoryManager(allocator);
        };

        return Arrays.asList(new Object[][] {
                { s1 },
                { s2 },
                { s3 }
        });
    }

    public SanityMemoryManagerTest(Supplier<MemoryManager> supplier) {
        this.supplier = supplier;

    }
    
    @Before
    public void setUp() {
        memoryManager = supplier.get();
    }
    

    @After
    public void tearDown() {
        memoryManager.clear(true);
        BlocksPool.clear();
    }

    @Test
    public void allocate() {
        BlockAllocationSlice s = (BlockAllocationSlice) memoryManager.getEmptySlice();
        ByteBuffer bb;

        s.allocate(4, false);
        Assert.assertEquals(4 + memoryManager.getHeaderSize(), s.getAllocatedLength());
        Assert.assertEquals(4 + memoryManager.getHeaderSize() , memoryManager.allocated());

        s.allocate(4, false);
        Assert.assertEquals(4 + memoryManager.getHeaderSize(), s.getAllocatedLength());
        Assert.assertEquals(2 * (4 + memoryManager.getHeaderSize()), memoryManager.allocated());
    }
}
