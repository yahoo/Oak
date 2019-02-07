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
    private OakMemoryAllocator memoryAllocator;
    private ThreadIndexCalculator indexCalculator;
    private MemoryManager memoryManager;
    private long allocatedBytes;

    @Before
    public void setUp() {
        allocatedBytes = 0;
        indexCalculator = mock(ThreadIndexCalculator.class);
        when(indexCalculator.getIndex()).thenReturn(7);
        memoryAllocator = mock(OakMemoryAllocator.class);
        when(memoryAllocator.allocate(anyInt())).thenAnswer((Answer) invocation -> {
            int size = (int) invocation.getArguments()[0];
            allocatedBytes += size;
            return ByteBuffer.allocate(size);

        });
        doAnswer(invocation -> {
            ByteBuffer bb = (ByteBuffer) invocation.getArguments()[0];
            allocatedBytes -= bb.capacity();
            return  allocatedBytes;
        }).when(memoryAllocator).free(any());
        when(memoryAllocator.allocated()).thenAnswer((Answer) invocationOnMock -> allocatedBytes);
        memoryManager = new MemoryManager(memoryAllocator, indexCalculator);
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
    public void release() {
        memoryManager.setGCtrigger(2);

        ByteBuffer bb1 = memoryManager.allocate(4);
        ByteBuffer bb2 = memoryManager.allocate(4);

        memoryManager.release(bb1);
        verify(memoryAllocator, times(0)).free(any());
        assertEquals(8, memoryManager.allocated());

        memoryManager.release(bb2);
        verify(memoryAllocator, times(2)).free(any());
        assertEquals(0, memoryManager.allocated());
    }


    @Test
    public void close() {
        memoryManager.setGCtrigger(2);

        ByteBuffer bb1 = memoryManager.allocate(4);
        memoryManager.release(bb1);
        verify(memoryAllocator, times(0)).free(any());
        assertEquals(4, memoryManager.allocated());

        memoryManager.close();
        verify(memoryAllocator, times(1)).free(any());
        assertEquals(0, memoryManager.allocated());
    }



}
