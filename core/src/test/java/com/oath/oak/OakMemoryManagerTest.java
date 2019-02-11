/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class OakMemoryManagerTest {
    private OakMemoryAllocator valuesMemoryAllocator;
    private OakMemoryAllocator keysMemoryAllocator;

    private MemoryManager memoryManager;
    private long allocatedBytes;

    @Before
    public void setUp() {
        allocatedBytes = 0;
        valuesMemoryAllocator = mock(OakMemoryAllocator.class);
        keysMemoryAllocator = new DirectMemoryAllocator();
        when(valuesMemoryAllocator.allocate(anyInt())).thenAnswer((Answer) invocation -> {
            int size = (int) invocation.getArguments()[0];
            allocatedBytes += size;
            return ByteBuffer.allocate(size);

        });
        doAnswer(invocation -> {
            ByteBuffer bb = (ByteBuffer) invocation.getArguments()[0];
            allocatedBytes -= bb.capacity();
            return  allocatedBytes;
        }).when(valuesMemoryAllocator).free(any());
        when(valuesMemoryAllocator.allocated()).thenAnswer((Answer) invocationOnMock -> allocatedBytes);
        memoryManager = new MemoryManager(valuesMemoryAllocator, keysMemoryAllocator);
    }

    @Test
    public void allocate() {
        ByteBuffer bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(4, memoryManager.allocated());

        bb = memoryManager.allocate(4);
        assertEquals(4, bb.remaining());
        assertEquals(8, memoryManager.allocated());
    }

    @Test
    public void allocateKeys() {
        ByteBuffer bb = memoryManager.allocateKeys(1024);
        assertEquals(1024, bb.remaining());
        assertEquals(1024, bb.capacity());
    }
}
