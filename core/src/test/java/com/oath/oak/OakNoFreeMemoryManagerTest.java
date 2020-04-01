/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class OakNoFreeMemoryManagerTest {

    private NoFreeMemoryManager noFreeMemoryManager;

    @Before
    public void setUp() {
        OakBlockMemoryAllocator keysMemoryAllocator = new OakNativeMemoryAllocator(128);
        noFreeMemoryManager = new NoFreeMemoryManager(keysMemoryAllocator);
    }

    @Test
    public void allocate() {
        ByteBuffer bb = noFreeMemoryManager.allocateSlice(4).getByteBuffer();
        assertEquals(4, bb.remaining());
        assertEquals(4, noFreeMemoryManager.allocated());

        bb = noFreeMemoryManager.allocateSlice(4).getByteBuffer();
        assertEquals(4, bb.remaining());
        assertEquals(8, noFreeMemoryManager.allocated());
    }
}
