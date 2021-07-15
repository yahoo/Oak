/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.integer.OakIntComparator;
import com.yahoo.oak.common.integer.OakIntSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class HashChunkNoSplitTest {
    private static final int MAX_ITEMS_PER_CHUNK = 20;
    private final ValueUtils valueOperator = new ValueUtils();
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
    SyncRecycleMemoryManager memoryManager = new SyncRecycleMemoryManager(allocator);
    OakIntSerializer serializer = new OakIntSerializer();

    // the put flow done by InternalOakHashMap
    private void put(ThreadContext ctx, HashChunk c, Integer key) {

        long previouslyAllocatedBytes = memoryManager.allocated();
        long oneMappingSizeInBytes =
            memoryManager.getHeaderSize() * 2 + serializer.calculateSize(key) * 2;
        int numberOfMappingsBefore = c.externalSize.get();

        ctx.invalidate();

        // look for a key on an empty chunk
        c.lookUp(ctx, key);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());

        // allocate an entry and write the key there
        c.allocateEntryAndWriteKey(ctx, key);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());

        // look for unfinished insert key once again
        c.lookUp(ctx, key);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());

        // allocate and write the value
        c.allocateValue(ctx, key + 1, false);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(memoryManager.allocated() - previouslyAllocatedBytes, oneMappingSizeInBytes);
        Assert.assertEquals(c.externalSize.get(), numberOfMappingsBefore); // no mapping is yet allocated

        // linearization point should be preceded with successfull publishing
        assert c.publish();

        // link value (connect it with the entry)
        ValueUtils.ValueResult vr = c.linkValue(ctx);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertEquals(vr, ValueUtils.ValueResult.TRUE);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(c.externalSize.get(), numberOfMappingsBefore + 1); // one mapping is allocated

        ctx.invalidate();
        c.unpublish();

        // look for the key that should be found now
        c.lookUp(ctx, key);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(c.externalSize.get(), numberOfMappingsBefore + 1); // one mapping is allocated
        Assert.assertEquals(memoryManager.allocated() - previouslyAllocatedBytes,
            oneMappingSizeInBytes);

        // check the value
        Result result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(key + 1, ((Integer) result.value).intValue());

        return;
    }

    @Test
    public void testSimpleSingleThread() {
        ThreadContext ctx = new ThreadContext(memoryManager, memoryManager);
        Integer keySmall = new Integer(5);
        Integer keyBig = new Integer( 12345678);

        // create new Hash Chunk
        HashChunk c = new HashChunk(
            MAX_ITEMS_PER_CHUNK, new AtomicInteger(0), memoryManager, memoryManager,
            new OakIntComparator(), serializer, serializer);

        // PUT including GET
        put(ctx, c, keySmall);
        put(ctx, c, keyBig);

    }

}
