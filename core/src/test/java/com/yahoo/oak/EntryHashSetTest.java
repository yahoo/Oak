/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


import com.yahoo.oak.common.integer.OakIntComparator;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.Test;



public class EntryHashSetTest {

    @Test
    public void testSingleInsert() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
        SyncRecycleMemoryManager memoryManager = new SyncRecycleMemoryManager(allocator);

        // create EntryHashSet
        EntryHashSet ehs =
            new EntryHashSet(memoryManager, memoryManager, 100,
                new OakIntSerializer(), new OakIntSerializer(), new OakIntComparator());

        ThreadContext ctx = new ThreadContext(memoryManager, memoryManager);
//        ehs.allocateKey(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
    }


}
