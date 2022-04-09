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

public class UnscopedValueBufferSyncedTest {

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
    public void byteBufferShouldBeReadOnly() {
        Slice keySlice = sliceUtils.getEmptySlice();
        keySlice.allocate(100, false);
        Slice valueSlice = sliceUtils.getEmptySlice();
        valueSlice.allocate(100, false);

        KeyBuffer keyBuffer = new KeyBuffer(keySlice);
        ValueBuffer valueBuffer = new ValueBuffer(valueSlice);
        Assert.assertTrue(
                new UnscopedValueBufferSynced(keyBuffer, valueBuffer, null).getByteBuffer()
                        .isReadOnly());
    }
}
