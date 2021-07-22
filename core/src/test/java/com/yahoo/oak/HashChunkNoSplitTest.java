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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class HashChunkNoSplitTest {
    private static final int MAX_ITEMS_PER_CHUNK = 20;
    private final ValueUtils valueOperator = new ValueUtils();
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
    SyncRecycleMemoryManager memoryManager = new SyncRecycleMemoryManager(allocator);
    OakIntSerializer serializer = new OakIntSerializer();

    private final HashChunk c = new HashChunk(
        MAX_ITEMS_PER_CHUNK, new AtomicInteger(0), memoryManager, memoryManager,
        new OakIntComparator(), serializer, serializer);

    // the put flow done by InternalOakHashMap
    private void putNotExisting(Integer key, ThreadContext ctx, boolean concurrent) {

        long previouslyAllocatedBytes = memoryManager.allocated();
        long oneMappingSizeInBytes =
            memoryManager.getHeaderSize() * 2 + serializer.calculateSize(key) * 2;
        int numberOfMappingsBefore = c.externalSize.get();

        ctx.invalidate();

        // look for a key that should not be existing in the chunk
        c.lookUp(ctx, key);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());

        // allocate an entry and write the key there
        // (true should be returned, no rebalance should be requested)
        assert c.allocateEntryAndWriteKey(ctx, key);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());

        if (!concurrent) { // for concurrency we cannot change the thread context values
            // look for unfinished insert key once again
            c.lookUp(ctx, key);
            Assert.assertEquals(ctx.entryState, EntryArray.EntryState.INSERT_NOT_FINALIZED);
            Assert.assertTrue(ctx.isKeyValid());
            Assert.assertFalse(ctx.isValueValid());
            Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
            Assert.assertEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        }

        // allocate and write the value
        c.allocateValue(ctx, key + 1, false);
        Assert.assertTrue(ctx.entryState == EntryArray.EntryState.INSERT_NOT_FINALIZED
            || ctx.entryState == EntryArray.EntryState.UNKNOWN);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        if (!concurrent) {
            Assert.assertEquals(memoryManager.allocated() - previouslyAllocatedBytes, oneMappingSizeInBytes);
        }
        if (!concurrent) {
            Assert.assertEquals(c.externalSize.get(), numberOfMappingsBefore); // no mapping is yet allocated
        }

        // linearization point should be preceded with successfull publishing
        assert c.publish();

        // link value (connect it with the entry)
        ValueUtils.ValueResult vr = c.linkValue(ctx);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertEquals(vr, ValueUtils.ValueResult.TRUE);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        if (!concurrent) {
            Assert.assertEquals(c.externalSize.get(), numberOfMappingsBefore + 1); // one mapping is allocated
        }

        ctx.invalidate();
        c.unpublish();

        // look for the key that should be found now
        c.lookUp(ctx, key);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        if (!concurrent) {
            Assert.assertEquals(c.externalSize.get(), numberOfMappingsBefore + 1); // one mapping is allocated
        }
        if (!concurrent) {
            Assert.assertEquals(memoryManager.allocated() - previouslyAllocatedBytes,
                oneMappingSizeInBytes);
        }

        // check the value
        Result result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(key + 1, ((Integer) result.value).intValue());

        return;
    }

    private void deleteExisting(Integer key, ThreadContext ctx) {
        // delete firstly inserted entries, first look for a key and mark its value as deleted
        c.lookUp(ctx, key);
        Assert.assertNotEquals(ctx.entryIndex, EntryArray.INVALID_ENTRY_INDEX);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        Result result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(key + 1, ((Integer) result.value).intValue());

        ValueUtils.ValueResult vr = ctx.value.s.logicalDelete();
        assert vr == ValueUtils.ValueResult.TRUE;

        // look for the entry again, to ensure the state is delete not finalize
        // delete some entries, first look for a key and mark its value as deleted
        c.lookUp(ctx, key);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED_NOT_FINALIZED);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertFalse(ctx.isValueValid());

        // expect false because no rebalance should be requested. Includes publish/unpublish
        assert !c.finalizeDeletion(ctx);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
        Assert.assertEquals(ctx.key.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), memoryManager.getInvalidReference());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertFalse(ctx.isKeyValid());

        // look for a key that should not be existing in the chunk
        c.lookUp(ctx, key);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
    }

    @Test
    public void testSimpleSingleThread() {
        ThreadContext ctx = new ThreadContext(memoryManager, memoryManager);

        Integer keySmall = new Integer(5);
        Integer keyBig = new Integer( 12345678);
        Integer keyZero = new Integer(0);
        Integer keyNegative = new Integer(-123);
        Integer keySmallNegative = new Integer(-5); // same hash as 5

        // PUT including GET
        putNotExisting(keySmall, ctx, false);
        putNotExisting(keyBig, ctx, false);
        putNotExisting(keyZero, ctx, false);
        putNotExisting(keyNegative, ctx, false);
        putNotExisting(keySmallNegative, ctx, false);

        // DELETE
        deleteExisting(keySmall, ctx);
        deleteExisting(keyBig, ctx);
        deleteExisting(keyZero, ctx);
        deleteExisting(keyNegative, ctx);
        deleteExisting(keySmallNegative, ctx);

    }

    @Test(timeout = 5000)
    public void testSimpleMultiThread() throws InterruptedException {

        ThreadContext ctx = new ThreadContext(memoryManager, memoryManager);

        Integer keyFirst = new Integer(5);
        Integer keySecond = new Integer(6);
        Integer keyThird = new Integer(7);
        Integer keyFirstNegative = new Integer(-5); // same hash as 5
        Integer keySecondNegative = new Integer(-6); // same hash as 6

        // Parties: test thread and inserter thread
        CyclicBarrier barrier = new CyclicBarrier(2);

        Thread inserter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            ThreadContext ctxInserter = new ThreadContext(memoryManager, memoryManager);
            putNotExisting(keyFirst, ctxInserter, true);
            putNotExisting(keySecond, ctxInserter, true);
            c.lookUp(ctxInserter, keySecond);
            Assert.assertTrue(ctxInserter.isKeyValid());
            Assert.assertTrue(ctxInserter.isValueValid());
            c.lookUp(ctxInserter, keyFirst);
            Assert.assertTrue(ctxInserter.isKeyValid());
            Assert.assertTrue(ctxInserter.isValueValid());

            putNotExisting(keyThird, ctxInserter, true);
            c.lookUp(ctxInserter, keyThird);
            Assert.assertTrue(ctxInserter.isKeyValid());
            Assert.assertTrue(ctxInserter.isValueValid());

            deleteExisting(keyThird, ctxInserter);
            c.lookUp(ctxInserter, keyThird);
            Assert.assertFalse(ctxInserter.isKeyValid());
            Assert.assertFalse(ctxInserter.isValueValid());

        });

        inserter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        putNotExisting(keyFirstNegative, ctx, true);
        putNotExisting(keySecondNegative, ctx, true);

        // look for a key that should be existing in the chunk
        c.lookUp(ctx, keyFirstNegative);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        // look for a key that should be existing in the chunk
        c.lookUp(ctx, keySecondNegative);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());

        deleteExisting(keyFirstNegative, ctx);
        c.lookUp(ctx, keyFirstNegative);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());

        inserter.join();

        // Not-concurrently look for all keys that should be existing in the chunk
        c.lookUp(ctx, keyFirst);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        // look for a key that should be existing in the chunk
        c.lookUp(ctx, keySecond);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        c.lookUp(ctx, keyThird);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
        // look for a key that should be existing in the chunk
        c.lookUp(ctx, keyFirstNegative);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
        // look for a key that should be existing in the chunk
        c.lookUp(ctx, keySecondNegative);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
    }

}
