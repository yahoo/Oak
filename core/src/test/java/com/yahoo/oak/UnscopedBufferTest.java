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

public class UnscopedBufferTest {

    private SliceUtils sliceUtils;

    @Before
    public void setUp() throws Exception {
        sliceUtils = new SliceUtils();
    }

    @After
    public void tearDown() throws Exception {
        sliceUtils.close();
    }

    @Test
    public void byteBufferShouldBeReadOnlyWhenWrappingReadOnlyBuffer() {
        Slice s = sliceUtils.getEmptySlice();
        s.allocate(100, false);
        ScopedReadBuffer readBuffer = new ScopedReadBuffer(s);
        Assert.assertTrue(new UnscopedBuffer<>(readBuffer).getByteBuffer().isReadOnly());
    }

    @Test
    public void byteBufferShouldBeWritableWhenWrappingWritableBuffer() {
        Slice s = sliceUtils.getEmptySlice();
        s.allocate(100, false);
        ScopedWriteBuffer.compute(s, buf -> Assert.assertFalse(
                new UnscopedBuffer<>((ScopedWriteBuffer) buf).getByteBuffer().isReadOnly()));
    }
}
