package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
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
            allocatedSlices[i] = novaManager.allocateSlice(i + 5, false).duplicate();
        }
        for (int i = 0; i < NovaManager.RELEASE_LIST_LIMIT; i++) {
            assertEquals(i + 5, allocatedSlices[i].getByteBuffer().remaining());
            novaManager.releaseSlice(allocatedSlices[i]);
        }
        assertEquals(NovaManager.RELEASE_LIST_LIMIT, allocator.getFreeListLength());
        long newVersion = novaManager.getCurrentVersion();
        assertEquals(oldVersion + 1, newVersion);
        for (int i = NovaManager.RELEASE_LIST_LIMIT - 1; i > -1; i--) {
            Slice s = novaManager.allocateSlice(i + 5, false);
            assertEquals(allocatedSlices[i].getBlockID(), s.getBlockID());
            assertEquals(allocatedSlices[i].getByteBuffer().position(), s.getByteBuffer().position());
            assertEquals(allocatedSlices[i].getByteBuffer().limit(), s.getByteBuffer().limit());
        }
    }
}
