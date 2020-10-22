/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Test;

public class NativeManagerTest {

    @Test
    public void reuseTest() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
        NativeMemoryManager novaManager = new NativeMemoryManager(allocator);
        long oldVersion = novaManager.getCurrentVersion();
        Slice[] allocatedSlices = new Slice[NativeMemoryManager.RELEASE_LIST_LIMIT];
        for (int i = 0; i < NativeMemoryManager.RELEASE_LIST_LIMIT; i++) {
            allocatedSlices[i] = novaManager.getEmptySlice();
            novaManager.allocate(allocatedSlices[i], i + 5, false);
            allocatedSlices[i].duplicateBuffer();
        }
        for (int i = 0; i < NativeMemoryManager.RELEASE_LIST_LIMIT; i++) {
            Assert.assertEquals(i + 5, allocatedSlices[i].getLength());
            novaManager.release(allocatedSlices[i]);
        }
        Assert.assertEquals(NativeMemoryManager.RELEASE_LIST_LIMIT, allocator.getFreeListLength());
        long newVersion = novaManager.getCurrentVersion();
        Assert.assertEquals(oldVersion + 1, newVersion);
        for (int i = NativeMemoryManager.RELEASE_LIST_LIMIT - 1; i > -1; i--) {
            Slice s = novaManager.getEmptySlice();
            novaManager.allocate(s, i + 5, false);
            Assert.assertEquals(allocatedSlices[i].getAllocatedBlockID(), s.getAllocatedBlockID());
            Assert.assertEquals(allocatedSlices[i].getAllocatedLength(), s.getAllocatedLength());
            Assert.assertEquals(allocatedSlices[i].getAllocatedOffset(), s.getAllocatedOffset());
        }
    }
}
