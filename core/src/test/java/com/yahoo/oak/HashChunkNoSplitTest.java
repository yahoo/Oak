/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class HashChunkNoSplitTest {
    OakSharedConfig<Integer, Integer> config;
    private static final int MAX_ITEMS_PER_CHUNK = 64;

    private final UnionCodec hashIndexCodec = new UnionCodec(
            5, // the size of the first, as these are LSBs
            UnionCodec.AUTO_CALCULATE_BIT_SIZE,  // the second (MSB) will be calculated
            Integer.SIZE
    );

    private HashChunk<Integer, Integer> c;

    @Before
    public void setUp() {
        NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
        SyncRecycleMemoryManager memoryManager = new SyncRecycleMemoryManager(allocator);
        config = OakCommonBuildersFactory.getDefaultIntBuilder().buildSharedConfig(
                allocator, memoryManager, memoryManager
        );
        c = new HashChunk<>(config, MAX_ITEMS_PER_CHUNK, hashIndexCodec);
    }

    @After
    public void tearDown() {
        config.memoryAllocator.close();
        BlocksPool.clear();
    }

    ThreadContext getCtx() {
        return new ThreadContext(config);
    }

    // the put flow done by InternalOakHashMap
    private void putNotExisting(Integer key, ThreadContext ctx, boolean concurrent) {

        long previouslyAllocatedBytes = config.keysMemoryManager.allocated();
        long oneMappingSizeInBytes =
                config.keysMemoryManager.getHeaderSize() * 2L + config.keySerializer.calculateSize(key) * 2L;
        int numberOfMappingsBefore = c.config.size.get();

        ctx.invalidate();

        // look for a key that should not be existing in the chunk
        c.lookUp(ctx, key);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());

        // allocate an entry and write the key there
        // (true should be returned, no rebalance should be requested)
        Assert.assertTrue(c.allocateEntryAndWriteKey(ctx, key));

        if (concurrent) { // for concurrency the entry state can be also deleted
            // (from this or other key being previously inserted and fully deleted)
            Assert.assertTrue(ctx.entryState == EntryArray.EntryState.DELETED
                || ctx.entryState == EntryArray.EntryState.UNKNOWN);
        } else {
            Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        }
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());

        if (!concurrent) { // for concurrency, we cannot change the thread context values
            // look for unfinished insert key once again
            c.lookUp(ctx, key);
            Assert.assertEquals(ctx.entryState, EntryArray.EntryState.INSERT_NOT_FINALIZED);
            Assert.assertTrue(ctx.isKeyValid());
            Assert.assertFalse(ctx.isValueValid());
            Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
            Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        }

        // allocate and write the value
        c.allocateValue(ctx, key + 1, false);
        Assert.assertTrue(ctx.entryState == EntryArray.EntryState.INSERT_NOT_FINALIZED
            || ctx.entryState == EntryArray.EntryState.UNKNOWN
            || ctx.entryState == EntryArray.EntryState.DELETED);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        if (!concurrent) {
            Assert.assertEquals(oneMappingSizeInBytes,
                    config.keysMemoryManager.allocated() - previouslyAllocatedBytes);
        }
        if (!concurrent) {
            Assert.assertEquals(c.config.size.get(), numberOfMappingsBefore); // no mapping is yet allocated
        }

        // linearization point should be preceded with successful publishing
        Assert.assertTrue(c.publish());

        // link value (connect it with the entry)
        ValueUtils.ValueResult vr = c.linkValue(ctx);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertEquals(vr, ValueUtils.ValueResult.TRUE);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        if (!concurrent) {
            Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        }
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        if (!concurrent) {
            Assert.assertEquals(c.config.size.get(), numberOfMappingsBefore + 1); // one mapping is allocated
        }

        ctx.invalidate();
        c.unpublish();

        // look for the key that should be found now
        c.lookUp(ctx, key);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        if (!concurrent) {
            Assert.assertEquals(c.config.size.get(), numberOfMappingsBefore + 1); // one mapping is allocated
        }
        if (!concurrent) {
            Assert.assertEquals(config.valuesMemoryManager.allocated() - previouslyAllocatedBytes,
                oneMappingSizeInBytes);
        }

        // check the value
        Result result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(key + 1, ((Integer) result.value).intValue());
    }

    private void deleteExisting(Integer key, ThreadContext ctx, boolean concurrent) {
        // delete firstly inserted entries, first look for a key and mark its value as deleted
        c.lookUp(ctx, key);
        Assert.assertNotEquals(ctx.entryIndex, EntryArray.INVALID_ENTRY_INDEX);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        Result result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(key + 1, ((Integer) result.value).intValue());

        ValueUtils.ValueResult vr = ctx.value.s.logicalDelete();
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, vr);
        ctx.entryState = EntryArray.EntryState.DELETED_NOT_FINALIZED;

        // expect false because no rebalance should be requested. Includes publish/unpublish
        Assert.assertFalse(c.finalizeDeletion(ctx));
        if (!concurrent) {
            Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
            Assert.assertEquals("\nKey reference is " + ctx.key.getSlice().getReference()
                + " and not invalid reference", ctx.key.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
            Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
            Assert.assertEquals(ctx.newValue.getSlice().getReference(),
                    config.valuesMemoryManager.getInvalidReference());
            Assert.assertFalse(ctx.isValueValid());
            Assert.assertFalse(ctx.isKeyValid());
        }

        // look for a key that should not be existing in the chunk
        c.lookUp(ctx, key);
        Assert.assertFalse(ctx.isKeyValid());
        Assert.assertFalse(ctx.isValueValid());
    }

    @Test
    public void testSimpleSingleThread() {
        Integer keySmall = 5;
        Integer keyBig = 12345678;
        Integer keyZero = 0;
        Integer keyNegative = -123;
        Integer keySmallNegative = -5; // same hash as 5

        // PUT including GET
        ThreadContext ctx = getCtx();
        putNotExisting(keySmall, ctx, false);
        putNotExisting(keyBig, ctx, false);
        putNotExisting(keyZero, ctx, false);
        putNotExisting(keyNegative, ctx, false);
        putNotExisting(keySmallNegative, ctx, false);

        // DELETE
        deleteExisting(keySmall, ctx, false);
        deleteExisting(keyBig, ctx, false);
        deleteExisting(keyZero, ctx, false);
        deleteExisting(keyNegative, ctx, false);
        deleteExisting(keySmallNegative, ctx, false);

    }

    @Test(timeout = 5000)
    public void testSimpleMultiThread() throws InterruptedException {
        int numberOfMappingsBefore = c.config.size.get();

        Integer keyFirst = 5;
        Integer keySecond = 6;
        Integer keyThird = 7;
        Integer keyFirstNegative = -5; // same hash as 5
        Integer keySecondNegative = -6; // same hash as 6

        // Parties: test thread and inserter thread
        CyclicBarrier barrier = new CyclicBarrier(2);

        Thread inserter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            ThreadContext ctxInserter = getCtx();
            putNotExisting(keyFirst, ctxInserter, true);
            putNotExisting(keySecond, ctxInserter, true);
            c.lookUp(ctxInserter, keySecond);
            Assert.assertTrue(ctxInserter.isKeyValid());
            Assert.assertTrue(ctxInserter.isValueValid());
            Result result = config.valueOperator.transform(
                new Result(), ctxInserter.value, config.valueSerializer::deserialize);
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
            Assert.assertEquals(keySecond + 1, ((Integer) result.value).intValue());

            c.lookUp(ctxInserter, keyFirst);
            Assert.assertTrue(ctxInserter.isKeyValid());
            Assert.assertTrue(ctxInserter.isValueValid());
            result = config.valueOperator.transform(
                new Result(), ctxInserter.value, config.valueSerializer::deserialize);
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
            Assert.assertEquals(keyFirst + 1, ((Integer) result.value).intValue());

            putNotExisting(keyThird, ctxInserter, true);
            c.lookUp(ctxInserter, keyThird);
            Assert.assertTrue(ctxInserter.isKeyValid());
            Assert.assertTrue(ctxInserter.isValueValid());
            result = config.valueOperator.transform(
                new Result(), ctxInserter.value, config.valueSerializer::deserialize);
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
            Assert.assertEquals(keyThird + 1, ((Integer) result.value).intValue());

            deleteExisting(keyThird, ctxInserter, true);
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

        ThreadContext ctx = getCtx();
        putNotExisting(keyFirstNegative, ctx, true);
        putNotExisting(keySecondNegative, ctx, true);

        // look for a key that should be existing in the chunk
        c.lookUp(ctx, keyFirstNegative);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        Result result = config.valueOperator.transform(
            new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(keyFirstNegative + 1, ((Integer) result.value).intValue());

        // look for a key that should be existing in the chunk
        c.lookUp(ctx, keySecondNegative);
        Assert.assertTrue(ctx.isKeyValid());
        Assert.assertTrue(ctx.isValueValid());
        result = config.valueOperator.transform(
            new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(keySecondNegative + 1, ((Integer) result.value).intValue());

        deleteExisting(keyFirstNegative, ctx, true);
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

        Assert.assertEquals(c.config.size.get(), numberOfMappingsBefore + 3); // no mapping is yet allocated
    }

    @Test(timeout = 5000)
    public void testMultiThread() throws InterruptedException {
        ThreadContext ctx = getCtx();
        int numberOfMappingsBefore = c.config.size.get();

        // Parties: test thread and inserter thread
        CyclicBarrier barrier = new CyclicBarrier(2);

        Thread inserter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            ThreadContext ctxInserter = getCtx();
            // do not start from zero, this way two threads will not insert the same key
            // (inserting the same key simultaneously is not supported yet)
            for (int i = 1; i < MAX_ITEMS_PER_CHUNK; i += 5 ) {
                Integer key = i;
                putNotExisting(key, ctxInserter, true);
                c.lookUp(ctxInserter, key);
                Assert.assertTrue(ctxInserter.isKeyValid());
                Assert.assertTrue(ctxInserter.isValueValid());
                Result result = config.valueOperator.transform(
                    new Result(), ctxInserter.value, config.valueSerializer::deserialize);
                Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
                Assert.assertEquals(key + 1, ((Integer) result.value).intValue());
            }
            for (int i = 1; i < MAX_ITEMS_PER_CHUNK; i += 5 ) {
                Integer key = i;
                deleteExisting(key, ctxInserter, true);
                c.lookUp(ctxInserter, key);
                Assert.assertFalse(ctxInserter.isKeyValid());
                Assert.assertFalse(ctxInserter.isValueValid());
            }
        });

        inserter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }

        // do not start from zero, this way two threads will not insert the same key
        // (inserting the same key simultaneously is not supported yet)
        for (int i = 1; i < MAX_ITEMS_PER_CHUNK; i += 5 ) {
            Integer key = -i;
            putNotExisting(key, ctx, true);
            c.lookUp(ctx, key);
            Assert.assertTrue(ctx.isKeyValid());
            Assert.assertTrue(ctx.isValueValid());
            Result result = config.valueOperator.transform(
                new Result(), ctx.value, config.valueSerializer::deserialize);
            Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
            Assert.assertEquals(key + 1, ((Integer) result.value).intValue());
        }
        int numberOfMappingsBeforeThisThreadDeletes = c.config.size.get();
        Assert.assertTrue(numberOfMappingsBeforeThisThreadDeletes < MAX_ITEMS_PER_CHUNK);
        Assert.assertTrue(numberOfMappingsBeforeThisThreadDeletes > (MAX_ITEMS_PER_CHUNK / 5));

        for (int i = 1; i < MAX_ITEMS_PER_CHUNK; i += 5 ) {
            Integer key = -i;
            deleteExisting(key, ctx, true);
            c.lookUp(ctx, key);
            Assert.assertFalse(ctx.isKeyValid());
            Assert.assertFalse(ctx.isValueValid());
        }

        inserter.join();

        Assert.assertEquals("Size before test: " + numberOfMappingsBefore
                + ", size in the middle: " + numberOfMappingsBeforeThisThreadDeletes + ", size after: "
                + c.config.size.get() + ", statistics chunk size: " + c.statistics.getTotalCount(),
            c.config.size.get(), numberOfMappingsBefore);
    }

}
