package com.oath.oak;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NovaManagerTest {

    @Test
    public void reuseTest() {
        final OakNativeMemoryAllocator allocator = new OakNativeMemoryAllocator(128);
        NovaManager novaManager = new NovaManager(allocator);
        long oldVersion = novaManager.getCurrentVersion();
        Slice[] allocatedSlices = new Slice[NovaManager.RELEASE_LIST_LIMIT];
        for (int i = 0; i < NovaManager.RELEASE_LIST_LIMIT; i++) {
            allocatedSlices[i] = new Slice();
            novaManager.allocate(allocatedSlices[i], i + 5, MemoryManager.Allocate.VALUE);
            allocatedSlices[i].duplicateBuffer();
        }
        for (int i = 0; i < NovaManager.RELEASE_LIST_LIMIT; i++) {
            assertEquals(i + 5, allocatedSlices[i].getDataByteBuffer().remaining());
            novaManager.release(allocatedSlices[i]);
        }
        assertEquals(NovaManager.RELEASE_LIST_LIMIT, allocator.getFreeListLength());
        long newVersion = novaManager.getCurrentVersion();
        assertEquals(oldVersion + 1, newVersion);
        for (int i = NovaManager.RELEASE_LIST_LIMIT - 1; i > -1; i--) {
            Slice s = new Slice();
            novaManager.allocate(s, i + 5, MemoryManager.Allocate.VALUE);
            assertEquals(allocatedSlices[i].getAllocatedBlockID(), s.getAllocatedBlockID());
            assertEquals(allocatedSlices[i].getDataByteBuffer().position(), s.getDataByteBuffer().position());
            assertEquals(allocatedSlices[i].getDataByteBuffer().limit(), s.getDataByteBuffer().limit());
        }
    }
}
