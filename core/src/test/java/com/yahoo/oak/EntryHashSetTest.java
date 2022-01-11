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

public class EntryHashSetTest {
    OakSharedConfig<Integer, Integer> config;
    EntryHashSet<Integer, Integer> ehs;
    ThreadContext ctx;

    @Before
    public void setUp() {
        NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
        SyncRecycleMemoryManager memoryManager = new SyncRecycleMemoryManager(allocator);
        config = OakCommonBuildersFactory.getDefaultIntBuilder().buildSharedConfig(
                allocator, memoryManager, memoryManager
        );

        // create EntryHashSet
        ehs = new EntryHashSet<>(config, 100);
        ctx = new ThreadContext(config);
    }

    @After
    public void tearDown() {
        config.memoryAllocator.close();
        BlocksPool.clear();
    }

    private void allocateSimpleKeyValue() {
        // simple one key insert
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, 5, 7 /*000111*/, 39 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());

        // simple value allocation
        ehs.allocateValue(ctx, 50, false);
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());

        // commit the value, insert linearization points
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));

        /* ***********----- New insert ----************ */
        // simple another insert to the same hash idx different full hash idx
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, 15, 7 /*000111*/, 23 /*010111*/ ));
        Assert.assertEquals(ctx.entryIndex, 8);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());

        // add value allocation
        ehs.allocateValue(ctx, 150, false);
        Assert.assertEquals(ctx.entryIndex, 8);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ehs.getCollisionChainLength(), EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);

        // commit the value, insert linearization points
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));
    }

    private void allocateMoreKeyValue() {

        // insert different key with the same hash idx and the same full hash idx
        // (without exceeding default collision escape number)
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, 25, 7 /*000111*/, 23 /*010111*/ ));
        Assert.assertEquals(ctx.entryIndex, 9);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ehs.getCollisionChainLength(), EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);

        // add value allocation
        ehs.allocateValue(ctx, 250, false);
        Assert.assertEquals(ctx.entryIndex, 9);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ehs.getCollisionChainLength(), EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);

        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));


        // insert yet another different key with the same hash idx and the same full hash idx
        // (without exceeding (but reaching) the default collision escape number)
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, 55, 7 /*000111*/, 23 /*010111*/ ));
        Assert.assertEquals(ctx.entryIndex, 10);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ehs.getCollisionChainLength(), EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);

        // add value allocation
        ehs.allocateValue(ctx, 550, false);
        Assert.assertEquals(ctx.entryIndex, 10);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ehs.getCollisionChainLength(), EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);

        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));

        // insert different key with the same hash idx and the same full hash idx
        // (exceeding default collision escape number, but not all having the same full hash)
        // should fail to request a rebalance
        // TODO: temporarly disabling this test until rebalance is implement and
        // TODO: allocateEntryAndWriteKey functions accordingly
        // assert (!ehs.allocateEntryAndWriteKey(ctx, new Integer(35), 7 /*000111*/, 23 /*010111*/ ));

        // insert the same key again,
        // the valid entry state states that the key WASN'T inserted because the same key was found
        // The key and value buffers are populated with the found key
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, 5, 7 /*000111*/, 39 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());

        ctx.invalidate();
        ctx.entryIndex = 7;
        ctx.entryState = EntryArray.EntryState.VALID;
        // allocate another value to test double value commit for key 5 later
        ehs.allocateValue(ctx, 50, false);
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertEquals(ehs.getCollisionChainLength(), EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);
        Assert.assertEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());


        // should fail as mapping 5-->50 was already set
        Assert.assertEquals(ValueUtils.ValueResult.FALSE, ehs.writeValueCommit(ctx));

        // simple one insert, different hashIdx, same full hash idx
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, 4, 10 /*000111*/, 23 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 11);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());

        // add value allocation
        ehs.allocateValue(ctx, 40, false);
        Assert.assertEquals(ctx.entryIndex, 11);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertEquals(ehs.getCollisionChainLength(), EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());

        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));

        // insert different key with the same hash idx and the same full hash idx
        // (exceeding default collision escape number, triplet having the same full hash)
        // should not fail and increase collision escapes
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, 35, 8 /*000111*/, 23 /*010111*/ ));
        Assert.assertEquals(ctx.entryIndex, 12);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertTrue(ehs.getCollisionChainLength() > EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());

        // add value allocation
        ehs.allocateValue(ctx, 350, false);
        Assert.assertEquals(ctx.entryIndex, 12);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertTrue(ehs.getCollisionChainLength() > EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());

        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));
    }

    private void readKeyValue() {

        ctx.invalidate();

        Assert.assertTrue(ehs.lookUp(ctx, 5, 7 /*000111*/, 39 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        Result result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(50, ((Integer) result.value).intValue());

        // read from next hashIdx
        Assert.assertTrue(ehs.lookUp(ctx, 15, 7 /*000111*/, 23 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 8);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(150, ((Integer) result.value).intValue());

        // read from next next hashIdx
        Assert.assertTrue(ehs.lookUp(ctx, 25, 7 /*000111*/, 23 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 9);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(250, ((Integer) result.value).intValue());

        // look for not existing key
        Assert.assertFalse(ehs.lookUp(ctx, 3, 7 /*000111*/, 23 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, EntryArray.INVALID_ENTRY_INDEX);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());

        // look for existing key with different full hash index -> should not be found
        Assert.assertFalse(ehs.lookUp(ctx, 5, 7 /*000111*/, 11 ));
        Assert.assertEquals(ctx.entryIndex, EntryArray.INVALID_ENTRY_INDEX);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());

        // look for a key on within increased 'collision escapes' distance -> should be found
        Assert.assertTrue(ehs.lookUp(ctx, 35, 8 /*000111*/, 23 /*010111*/ ));
        Assert.assertEquals(12, ctx.entryIndex);
        Assert.assertEquals(EntryArray.EntryState.VALID, ctx.entryState);
        Assert.assertNotEquals(config.keysMemoryManager.getInvalidReference(), ctx.key.getSlice().getReference());
        Assert.assertNotEquals(config.valuesMemoryManager.getInvalidReference(), ctx.value.getSlice().getReference());
        Assert.assertTrue(ctx.isValueValid());
        Assert.assertEquals(ctx.entryIndex, 12);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(350, ((Integer) result.value).intValue());
    }

    // the main (single threaded) test flow
    @Test
    public void testSingleThread() {
        allocateSimpleKeyValue(); // very simple
        allocateMoreKeyValue(); // corner cases
        readKeyValue(); // next read values

        // delete firstly inserted entries, first look for a key and mark its value as deleted
        Assert.assertTrue(ehs.lookUp(ctx, 5, 7 /*000111*/, 39 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        Result result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(50, ((Integer) result.value).intValue());

        ValueUtils.ValueResult vr = ctx.value.s.logicalDelete(); //DELETE LINEARIZATION POINT
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, vr);
        ctx.entryState = EntryArray.EntryState.DELETED_NOT_FINALIZED;

        Assert.assertTrue(ehs.deleteValueFinish(ctx));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
        Assert.assertEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertFalse(ctx.isKeyValid());

        //look for the key once again to check it is not found
        Assert.assertFalse(ehs.lookUp(ctx, 5, 7 /*000111*/, 39 /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, EntryArray.INVALID_ENTRY_INDEX);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertFalse(ctx.isKeyValid());

        //insert on top of the deleted entry
        Integer key = 5;
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, key, 7 /*000111*/, key.hashCode() /*100111*/ ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(config.valuesMemoryManager.isReferenceDeleted(ctx.value.getSlice().getReference()));

        // simple value allocation
        ehs.allocateValue(ctx, 50, false);
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(config.valuesMemoryManager.isReferenceDeleted(ctx.value.getSlice().getReference()));
        Assert.assertTrue(ehs.getCollisionChainLength() > EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);

        // commit the value, insert linearization points
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));

        ctx.invalidate();

        // second delete
        // delete firstly inserted entries, first look for a key and mark its value as deleted
        Assert.assertTrue(ehs.lookUp(ctx, key, 7 /*000111*/, key.hashCode() ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(50, ((Integer) result.value).intValue());

        vr = ctx.value.s.logicalDelete();
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, vr);
        ctx.entryState = EntryArray.EntryState.DELETED_NOT_FINALIZED;

        Assert.assertTrue(ehs.deleteValueFinish(ctx));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
        Assert.assertEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertFalse(ctx.isKeyValid());

        //look for the key once again to check it is not found
        Assert.assertFalse(ehs.lookUp(ctx, key, 7, key.hashCode() ));
        Assert.assertEquals(ctx.entryIndex, EntryArray.INVALID_ENTRY_INDEX);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.UNKNOWN);
        Assert.assertEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertFalse(ctx.isValueValid());
        Assert.assertFalse(ctx.isKeyValid());

        //insert on top of the deleted entry
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, key, 7, key.hashCode() ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(config.valuesMemoryManager.isReferenceDeleted(ctx.value.getSlice().getReference()));

        // simple value allocation
        ehs.allocateValue(ctx, 50, false);
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.DELETED);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.newValue.getSlice().getReference(),
                config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(config.valuesMemoryManager.isReferenceDeleted(ctx.value.getSlice().getReference()));
        Assert.assertTrue(ehs.getCollisionChainLength() > EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH);

        // commit the value, insert linearization points
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, ehs.writeValueCommit(ctx));

        // one more delete
        // delete firstly inserted entries, first look for a key and mark its value as deleted
        Assert.assertTrue(ehs.lookUp(ctx, key, 7, key.hashCode() ));
        Assert.assertEquals(ctx.entryIndex, 7);
        Assert.assertEquals(ctx.entryState, EntryArray.EntryState.VALID);
        Assert.assertNotEquals(ctx.key.getSlice().getReference(), config.keysMemoryManager.getInvalidReference());
        Assert.assertNotEquals(ctx.value.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertEquals(ctx.newValue.getSlice().getReference(), config.valuesMemoryManager.getInvalidReference());
        Assert.assertTrue(ctx.isValueValid());

        result = config.valueOperator.transform(new Result(), ctx.value, config.valueSerializer::deserialize);
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(50, ((Integer) result.value).intValue());

        vr = ctx.value.s.logicalDelete();
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, vr);
        ctx.entryState = EntryArray.EntryState.DELETED_NOT_FINALIZED;

        Assert.assertTrue(ehs.isEntryDeleted(ctx.tempValue, 7));

        //try to insert on top of the not fully deleted entry!
        Assert.assertTrue(ehs.allocateEntryAndWriteKey(ctx, key, 7, key.hashCode() ));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            //Test access out of entry index bounds
            ehs.allocateEntryAndWriteKey(ctx, key, 7123456, key.hashCode() );
        });
    }
}
