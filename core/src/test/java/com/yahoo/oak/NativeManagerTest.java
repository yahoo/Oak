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
        SyncRecycleMemoryManager novaManager = new SyncRecycleMemoryManager(allocator);
        long oldVersion = novaManager.getCurrentVersion();
        Slice[] allocatedSlices = new Slice[SyncRecycleMemoryManager.RELEASE_LIST_LIMIT];
        for (int i = 0; i < SyncRecycleMemoryManager.RELEASE_LIST_LIMIT; i++) {
            allocatedSlices[i] = novaManager.getEmptySlice();
            allocatedSlices[i].allocate(i + 5, false);
        }
        for (int i = 0; i < SyncRecycleMemoryManager.RELEASE_LIST_LIMIT; i++) {
            Assert.assertEquals(i + 5, allocatedSlices[i].getLength());
            allocatedSlices[i].release();
        }
        Assert.assertEquals(SyncRecycleMemoryManager.RELEASE_LIST_LIMIT, allocator.getFreeListLength());
        long newVersion = novaManager.getCurrentVersion();
        Assert.assertEquals(oldVersion + 1, newVersion);
        for (int i = SyncRecycleMemoryManager.RELEASE_LIST_LIMIT - 1; i > -1; i--) {
            Slice s = novaManager.getEmptySlice();
            s.allocate(i + 5, false);
            Assert.assertEquals(allocatedSlices[i].getAllocatedBlockID(), s.getAllocatedBlockID());
            Assert.assertEquals(allocatedSlices[i].getAllocatedLength(), s.getAllocatedLength());
            Assert.assertEquals(allocatedSlices[i].getAllocatedOffset(), s.getAllocatedOffset());
        }
    }
}
