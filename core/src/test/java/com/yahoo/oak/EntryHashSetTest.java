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



public class EntryHashSetTest {
    private final ValueUtils valueOperator = new ValueUtils();

    private void allocateSimpleKeyValue(
        ThreadContext ctx, EntryHashSet ehs, SyncRecycleMemoryManager memoryManager) {

        // simple one key insert
        assert ehs.allocateKey(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
        assert ctx.entryIndex == 7 && ctx.entryState == EntryArray.EntryState.UNKNOWN
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference();

        // simple value allocation
        ehs.allocateValue(ctx, new Integer(50), false);
        assert ctx.entryIndex == 7 &&
            ctx.entryState == EntryArray.EntryState.UNKNOWN &&
            ehs.getCollisionChainLength() == EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.newValue.getSlice().getReference() != memoryManager.getInvalidReference();

        // commit the value, insert linearization points
        assert ehs.writeValueCommit(ctx) == ValueUtils.ValueResult.TRUE;

        /************----- New insert ----*************/
        // simple another insert to the same hash idx different full hash idx
        assert ehs.allocateKey(ctx, new Integer(15), 7 /*000111*/, 23 /*010111*/ );
        assert ctx.entryIndex == 8 && ctx.entryState == EntryArray.EntryState.UNKNOWN
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference();

        // add value allocation
        ehs.allocateValue(ctx, new Integer(150), false);
        assert ctx.entryIndex == 8 &&
            ctx.entryState == EntryArray.EntryState.UNKNOWN &&
            ehs.getCollisionChainLength() == EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.newValue.getSlice().getReference() != memoryManager.getInvalidReference();

        // commit the value, insert linearization points
        assert ehs.writeValueCommit(ctx) == ValueUtils.ValueResult.TRUE;
    }

    private void allocateMoreKeyValue(
        ThreadContext ctx, EntryHashSet ehs, SyncRecycleMemoryManager memoryManager) {

        // insert different key with the same hash idx and the same full hash idx
        // (without exceeding default collision escape number)
        assert ehs.allocateKey(ctx, new Integer(25), 7 /*000111*/, 23 /*010111*/ );
        assert ctx.entryIndex == 9 &&
            ctx.entryState == EntryArray.EntryState.UNKNOWN &&
            ehs.getCollisionChainLength() == EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference();

        // add value allocation
        ehs.allocateValue(ctx, new Integer(250), false);
        assert ctx.entryIndex == 9 &&
            ctx.entryState == EntryArray.EntryState.UNKNOWN &&
            ehs.getCollisionChainLength() == EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.newValue.getSlice().getReference() != memoryManager.getInvalidReference();

        assert ehs.writeValueCommit(ctx) == ValueUtils.ValueResult.TRUE;

        // insert different key with the same hash idx and the same full hash idx
        // (exceeding default collision escape number, but not all having the same full hash)
        // should fail to request a rebalance
        assert (!ehs.allocateKey(ctx, new Integer(35), 7 /*000111*/, 23 /*010111*/ ));

        // insert the same key again,
        // the valid entry state states that the key WASN'T inserted because the same key was found
        // The key buffer is populated with the found key
        assert ehs.allocateKey(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
        assert ctx.entryIndex == 7 && ctx.entryState == EntryArray.EntryState.VALID
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference();

        ctx.invalidate();
        ctx.entryIndex = 7;
        ctx.entryState = EntryArray.EntryState.VALID;
        // allocate another value to test double value commit for key 5 later
        ehs.allocateValue(ctx, new Integer(50), false);
        assert ctx.entryIndex == 7 &&
            ctx.entryState == EntryArray.EntryState.VALID &&
            ehs.getCollisionChainLength() == EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.newValue.getSlice().getReference() != memoryManager.getInvalidReference();

        // should fail as mapping 5-->50 was already set
        assert ehs.writeValueCommit(ctx) == ValueUtils.ValueResult.FALSE;

        // simple one insert, different hashIdx, same full hash idx
        assert ehs.allocateKey(ctx, new Integer(4), 10 /*000111*/, 23 /*100111*/ );
        assert ctx.entryIndex == 10 && ctx.entryState == EntryArray.EntryState.UNKNOWN
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference();

        // add value allocation
        ehs.allocateValue(ctx, new Integer(40), false);
        assert ctx.entryIndex == 10 &&
            ctx.entryState == EntryArray.EntryState.UNKNOWN &&
            ehs.getCollisionChainLength() == EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.newValue.getSlice().getReference() != memoryManager.getInvalidReference();

        assert ehs.writeValueCommit(ctx) == ValueUtils.ValueResult.TRUE;

        // insert different key with the same hash idx and the same full hash idx
        // (exceeding default collision escape number, triplet having the same full hash)
        // should not fail and increase collision escapes
        assert (ehs.allocateKey(ctx, new Integer(35), 8 /*000111*/, 23 /*010111*/ ));
        assert ctx.entryIndex == 11 &&
            ctx.entryState == EntryArray.EntryState.UNKNOWN &&
            ehs.getCollisionChainLength() > EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference();

        // add value allocation
        ehs.allocateValue(ctx, new Integer(350), false);
        assert ctx.entryIndex == 11 &&
            ctx.entryState == EntryArray.EntryState.UNKNOWN &&
            ehs.getCollisionChainLength() > EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.newValue.getSlice().getReference() != memoryManager.getInvalidReference();

        assert ehs.writeValueCommit(ctx) == ValueUtils.ValueResult.TRUE;
    }

    private void readKeyValue(
        ThreadContext ctx, EntryHashSet ehs, SyncRecycleMemoryManager memoryManager,
        OakIntSerializer serializer) {

        assert ehs.lookUp(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
        assert ctx.entryIndex == 7 && ctx.entryState == EntryArray.EntryState.VALID
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.isValueValid();
        Result result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(50, ((Integer) result.value).intValue());

        // read from next hashIdx
        assert ehs.lookUp(ctx, new Integer(15), 7 /*000111*/, 23 /*100111*/ );
        assert ctx.entryIndex == 8 && ctx.entryState == EntryArray.EntryState.VALID
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.isValueValid();
        result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(150, ((Integer) result.value).intValue());

        // read from next next hashIdx
        assert ehs.lookUp(ctx, new Integer(25), 7 /*000111*/, 23 /*100111*/ );
        assert ctx.entryIndex == 9 && ctx.entryState == EntryArray.EntryState.VALID
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.isValueValid();
        result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(250, ((Integer) result.value).intValue());

        // look for not existing key
        assert !ehs.lookUp(ctx, new Integer(3), 7 /*000111*/, 23 /*100111*/ );
        assert ctx.entryIndex == EntryArray.INVALID_ENTRY_INDEX && ctx.entryState == EntryArray.EntryState.UNKNOWN
            && ctx.key.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference();

        // look for existing key with different full hash index -> should not be found
        assert !ehs.lookUp(ctx, new Integer(5), 7 /*000111*/, 11 );
        assert ctx.entryIndex == EntryArray.INVALID_ENTRY_INDEX && ctx.entryState == EntryArray.EntryState.UNKNOWN
            && ctx.key.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference();

        // look for a key on within increased 'collision escapes' distance -> should be found
        assert ehs.lookUp(ctx, new Integer(35), 8 /*000111*/, 23 /*010111*/ );
        assert ctx.entryIndex == 11 && ctx.entryState == EntryArray.EntryState.VALID
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.isValueValid();
        result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(350, ((Integer) result.value).intValue());


    }

    // the main (single threaded) test flow
    @Test
    public void testSingleInsert() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
        SyncRecycleMemoryManager memoryManager = new SyncRecycleMemoryManager(allocator);
        OakIntSerializer serializer = new OakIntSerializer();

        // create EntryHashSet
        EntryHashSet ehs =
            new EntryHashSet(memoryManager, memoryManager, 100,
                serializer, serializer, new OakIntComparator());

        ThreadContext ctx = new ThreadContext(memoryManager, memoryManager);

        allocateSimpleKeyValue(ctx, ehs, memoryManager); // very simple
        allocateMoreKeyValue(ctx, ehs, memoryManager); // corner cases
        readKeyValue(ctx, ehs, memoryManager, serializer); // next read values

        // delete firstly inserted entries, first look for a key and mark its value as deleted
        assert ehs.lookUp(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
        assert ctx.entryIndex == 7 && ctx.entryState == EntryArray.EntryState.VALID
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.isValueValid();
        Result result = valueOperator.transform(new Result(), ctx.value, buf -> serializer.deserialize(buf));
        Assert.assertEquals(ValueUtils.ValueResult.TRUE, result.operationResult);
        Assert.assertEquals(50, ((Integer) result.value).intValue());

        ValueUtils.ValueResult vr = ctx.value.s.logicalDelete();
        assert vr == ValueUtils.ValueResult.TRUE;

        //look for the entry again, to ensure the state is delete not finalize
        // delete some entries, first look for a key and mark its value as deleted
        assert !ehs.lookUp(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
        assert ctx.entryIndex == 7 && ctx.entryState == EntryArray.EntryState.DELETED_NOT_FINALIZED
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() != memoryManager.getInvalidReference()
            && !ctx.isValueValid();

        assert ehs.deleteValueFinish(ctx);
        assert ctx.entryIndex == 7 && ctx.entryState == EntryArray.EntryState.DELETED
            && ctx.key.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && !ctx.isValueValid() && !ctx.isKeyValid();

        //look for the key once again to check it is not found
        assert !ehs.lookUp(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
        assert ctx.entryIndex == EntryArray.INVALID_ENTRY_INDEX && ctx.entryState == EntryArray.EntryState.UNKNOWN
            && ctx.key.getSlice().getReference() == memoryManager.getInvalidReference()
            && ctx.value.getSlice().getReference() == memoryManager.getInvalidReference()
            && !ctx.isValueValid() && !ctx.isKeyValid();

        //insert on top of the deleted entry
        assert ehs.allocateKey(ctx, new Integer(5), 7 /*000111*/, 39 /*100111*/ );
        assert ctx.entryIndex == 7 && ctx.entryState == EntryArray.EntryState.DELETED
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && memoryManager.isReferenceDeleted(ctx.value.getSlice().getReference());

        // simple value allocation
        ehs.allocateValue(ctx, new Integer(50), false);
        assert ctx.entryIndex == 7 &&
            ctx.entryState == EntryArray.EntryState.DELETED &&
            ehs.getCollisionChainLength() > EntryHashSet.DEFAULT_COLLISION_CHAIN_LENGTH
            && ctx.key.getSlice().getReference() != memoryManager.getInvalidReference()
            && memoryManager.isReferenceDeleted(ctx.value.getSlice().getReference())
            && ctx.newValue.getSlice().getReference() != memoryManager.getInvalidReference();

        // commit the value, insert linearization points
        assert ehs.writeValueCommit(ctx) == ValueUtils.ValueResult.TRUE;





    }


}
