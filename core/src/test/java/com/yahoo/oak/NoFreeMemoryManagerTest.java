/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class NoFreeMemoryManagerTest {

    private NoFreeMemoryManager noFreeMemoryManager;

    @Before
    public void setUp() {
        BlockMemoryAllocator keysMemoryAllocator = new NativeMemoryAllocator(128);
        noFreeMemoryManager = new NoFreeMemoryManager(keysMemoryAllocator);
    }

    @Test
    public void allocate() {
        Slice s = new Slice();
        ByteBuffer bb;

        noFreeMemoryManager.allocate(s, 4, MemoryManager.Allocate.KEY);
        Assert.assertEquals(4, s.getAllocatedLength());
        Assert.assertEquals(4, noFreeMemoryManager.allocated());

        noFreeMemoryManager.allocate(s, 4, MemoryManager.Allocate.KEY);
        Assert.assertEquals(4, s.getAllocatedLength());
        Assert.assertEquals(8, noFreeMemoryManager.allocated());
    }
}
