/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

class InternalOakMap<K, V>  extends InternalOakBasics<K, V> {

    /*-------------- Members --------------*/

    final ConcurrentSkipListMap<Object, OrderedChunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
    private final AtomicReference<OrderedChunk<K, V>> head;
    private final OakComparator<K> comparator;
    private final MemoryManager valuesMemoryManager;
    private final MemoryManager keysMemoryManager;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    // The reference count is used to count the upper objects wrapping this internal map:
    // OakMaps (including subMaps and Views) when all of the above are closed,
    // his map can be closed and memory released.
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private final ValueUtils valueOperator;

    /*-------------- Constructors --------------*/

    /**
     * init with capacity = 2g
     */

    InternalOakMap(K minKey, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
        OakComparator<K> oakComparator, MemoryManager vMM, MemoryManager kMM, int chunkMaxItems,
        ValueUtils valueOperator) {

        super(vMM, kMM);
        this.valuesMemoryManager = vMM;
        this.keysMemoryManager = kMM;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.comparator = oakComparator;

        // This is a trick for letting us search through the skiplist using both serialized and unserialized keys.
        // Might be nicer to replace it with a proper visitor
        Comparator<Object> mixedKeyComparator = (o1, o2) -> {
            if (o1 instanceof OakScopedReadBuffer) {
                if (o2 instanceof OakScopedReadBuffer) {
                    return oakComparator.compareSerializedKeys((OakScopedReadBuffer) o1, (OakScopedReadBuffer) o2);
                } else {
                    // Note the inversion of arguments, hence sign flip
                    return (-1) * oakComparator.compareKeyAndSerializedKey((K) o2, (OakScopedReadBuffer) o1);
                }
            } else {
                if (o2 instanceof OakScopedReadBuffer) {
                    return oakComparator.compareKeyAndSerializedKey((K) o1, (OakScopedReadBuffer) o2);
                } else {
                    return oakComparator.compareKeys((K) o1, (K) o2);
                }
            }
        };
        this.skiplist = new ConcurrentSkipListMap<>(mixedKeyComparator);

        OrderedChunk<K, V>
            head = new OrderedChunk<>(minKey, chunkMaxItems, this.size, vMM, kMM, this.comparator,
                keySerializer, valueSerializer);
        this.skiplist.put(head.minKey, head);    // add first orderedChunk (head) into skiplist
        this.head = new AtomicReference<>(head);
        this.valueOperator = valueOperator;
    }

    /*-------------- Closable --------------*/

    /**
     * cleans off heap memory
     */
    void close() {
        int res = referenceCount.decrementAndGet();
        // reference counter counts the submaps referencing the same InternalOakMap instance
        // once reference count is zeroed, the map meant to be deleted and should not be used.
        // reference count will never grow again
        if (res == 0) {
            super.close();
        }
    }

    // yet another object started to refer to this internal map
    void open() {
        while (true) {
            int res = referenceCount.get();
            // once reference count is zeroed, the map meant to be deleted and should not be used.
            // reference count should never grow again and the referral is not allowed
            if (res == 0) {
                throw new ConcurrentModificationException();
            }
            // although it is costly CAS is used here on purpose so we never increase
            // zeroed reference count
            if (referenceCount.compareAndSet(res, res + 1)) {
                break;
            }
        }
    }

    /*-------------- Methods --------------*/

    /**
     * finds and returns the orderedChunk where key should be located, starting from given orderedChunk
     */
    private OrderedChunk<K, V> iterateChunks(final OrderedChunk<K, V> inputOrderedChunk, K key) {
        // find orderedChunk following given orderedChunk (next)
        OrderedChunk<K, V> curr = inputOrderedChunk;
        OrderedChunk<K, V> next = curr.next.getReference();

        // since skiplist isn't updated atomically in split/compaction, our key might belong in the next orderedChunk
        // we need to iterate the chunks until we find the correct one
        while ((next != null) && (comparator.compareKeyAndSerializedKey(key, next.minKey) >= 0)) {
            curr = next;
            next = curr.next.getReference();
        }

        return curr;
    }

    @Override
    protected void rebalanceBasic(BasicChunk<K, V> basicChunk) {
        rebalance((OrderedChunk<K, V>) basicChunk); // exception will be triggered on wrong type
    }

    /**
     * @param c - OrderedChunk to rebalance
     */
    private void rebalance(OrderedChunk<K, V> c) {

        if (c == null) {
            return;
        }
        Rebalancer<K, V> rebalancer = new Rebalancer<>(c);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        ThreadContext ctx = getThreadContext();
        rebalancer.createNewChunks(ctx); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<OrderedChunk<K, V>> newOrderedChunks = rebalancer.getNewChunks();
        List<OrderedChunk<K, V>> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newOrderedChunks);

        updateIndexAndNormalize(engaged, newOrderedChunks);

        engaged.forEach(OrderedChunk::release);
    }

    private void connectToChunkList(List<OrderedChunk<K, V>> engaged, List<OrderedChunk<K, V>> children) {

        updateLastChild(engaged, children);
        int countIterations = 0;
        OrderedChunk<K, V> firstEngaged = engaged.get(0);

        // replace in linked list - we now need to find previous orderedChunk to our orderedChunk
        // and CAS its next to point to c1, which is the same c1 for all threads who reached this point
        // since prev might be marked (in compact itself) - we need to repeat this until successful
        while (true) {
            countIterations++;
            assert (countIterations < 10000); // this loop is not supposed to be infinite

            // start with first orderedChunk (i.e., head)
            Map.Entry<Object, OrderedChunk<K, V>> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

            OrderedChunk<K, V> prev = lowerEntry != null ? lowerEntry.getValue() : null;
            OrderedChunk<K, V> curr = (prev != null) ? prev.next.getReference() : null;

            // if didn't succeed to find prev through the skiplist - start from the head
            if (prev == null || curr != firstEngaged) {
                prev = null;
                curr = skiplist.firstEntry().getValue();    // TODO we can store&update head for a little efficiency
                // iterate until found orderedChunk or reached end of list
                while ((curr != firstEngaged) && (curr != null)) {
                    prev = curr;
                    curr = curr.next.getReference();
                }
            }

            // orderedChunk is head, we need to "add it to the list" for linearization point
            if (curr == firstEngaged && prev == null) {
                this.head.compareAndSet(firstEngaged, children.get(0));
                break;
            }
            // orderedChunk is not in list (someone else already updated list), so we're done with this part
            if (curr == null) {
                break;
            }

            // if prev orderedChunk is marked - it is deleted, need to help split it and then continue
            if (prev.next.isMarked()) {
                rebalance(prev);
                continue;
            }

            // try to CAS prev orderedChunk's next - from orderedChunk (that we split) into c1
            // c1 is the old orderedChunk's replacement, and is already connected to c2
            // c2 is already connected to old orderedChunk's next - so all we need to do is this replacement
            if ((prev.next.compareAndSet(firstEngaged, children.get(0), false, false)) ||
                    (!prev.next.isMarked())) {
                // if we're successful, or we failed but prev is not marked - so it means someone else was successful
                // then we're done with loop
                break;
            }
        }

    }

    private void updateLastChild(List<OrderedChunk<K, V>> engaged, List<OrderedChunk<K, V>> children) {
        OrderedChunk<K, V> lastEngaged = engaged.get(engaged.size() - 1);
        OrderedChunk<K, V> nextToLast = lastEngaged.markAndGetNext(); // also marks last engaged orderedChunk as deleted
        OrderedChunk<K, V> lastChild = children.get(children.size() - 1);

        lastChild.next.compareAndSet(null, nextToLast, false, false);
    }

    private void updateIndexAndNormalize(
        List<OrderedChunk<K, V>> engagedOrderedChunks, List<OrderedChunk<K, V>> children) {

        Iterator<OrderedChunk<K, V>> iterEngaged = engagedOrderedChunks.iterator();
        Iterator<OrderedChunk<K, V>> iterChildren = children.iterator();

        OrderedChunk<K, V> firstEngaged = iterEngaged.next();
        OrderedChunk<K, V> firstChild = iterChildren.next();

        // need to make the new chunks available, before removing old chunks
        skiplist.replace(firstEngaged.minKey, firstEngaged, firstChild);

        // remove all old chunks from index.
        while (iterEngaged.hasNext()) {
            OrderedChunk engagedToRemove = iterEngaged.next();
            skiplist.remove(engagedToRemove.minKey, engagedToRemove); // conditional remove is used
        }

        // for simplicity -  naive lock implementation
        // can be implemented without locks using versions on next pointer in skiplist
        while (iterChildren.hasNext()) {
            OrderedChunk<K, V> childToAdd = iterChildren.next();
            synchronized (childToAdd) {
                if (childToAdd.state() == BasicChunk.State.INFANT) { // make sure it wasn't add before
                    skiplist.putIfAbsent(childToAdd.minKey, childToAdd);
                    childToAdd.normalize();
                }
                // has a built in fence, so no need to add one here
            }
        }

        // now after removing old chunks and updating the skiplist, we can start normalizing
        firstChild.normalize();
    }

    // returns false when restart is needed
    // (if rebalance happened or another valid entry with same key was found)
    private boolean allocateAndLinkEntry(OrderedChunk c, ThreadContext ctx, K key, boolean isPutIfAbsent) {
        // There was no such key found, going to allocate a new key.
        // EntryOrderedSet allocates the entry (holding the key) and ctx is going to be updated
        // to be used by EntryOrderedSet's subsequent requests to write value
        if (!c.allocateEntryAndWriteKey(ctx, key)) {
            rebalance(c); // there was no space to allocate new entry, need to rebalance
            return false;     // after rebalance always restart
        }
        int prevEi = c.linkEntry(ctx, key);
        if (prevEi != ctx.entryIndex) {
            if (!isPutIfAbsent) {
                // our entry wasn't inserted because other entry with same key was found.
                // If this entry (with index prevEi) is valid we should move to continue
                // with existing entry scenario, otherwise we can reuse this entry because
                // its value is invalid.
                c.releaseKey(ctx);
                if (!c.isValueRefValidAndNotDeleted(prevEi)) {
                    return false;
                }
                // We use an existing entry only if its value reference is invalid
                ctx.entryIndex = prevEi;
            } else {
                // our entry wasn't inserted because other entry with same key was found. If this
                // entry (with index prevEi) is valid putIfAbsent should return ValueUtils.ValueResult.FALSE,
                // otherwise we can reuse this entry because its value is invalid.
                c.releaseKey(ctx);
                // to support linearization, a deeper validity check is needed
                // including off-heap delete bit check
                ctx.entryIndex = prevEi;
                c.readValue(ctx);
                if (ctx.isValueValid()) {
                    // If exists a matching value reference for the given key,
                    // and it isn't marked deleted, returning here false will cause the restart
                    // so the new value will be found and processed
                    return false;
                }
            }
        }
        return true;
    }

    /*-------------- OakMap Methods --------------*/

    // put the value assosiated with the key, if key existed old value is overwritten
    // TODO: organize the return values for ZC and non-ZC APIs
    V put(K key, V value, OakTransformer<V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);
            // If there is a matching value reference for the given key, and it is not marked as deleted,
            // then this put changes the slice pointed by this value reference.
            if (ctx.isValueValid()) {
                // there is a value and it is not deleted
                Result res = valueOperator.exchange(c, ctx, value, transformer, valueSerializer);
                if (res.operationResult == ValueUtils.ValueResult.TRUE) {
                    return (V) res.value;
                }
                // it might be that this chunk is proceeding with rebalance -> help
                helpRebalanceIfInProgress(c);
                // Exchange failed because the value was deleted/moved between lookup and exchange. Continue with
                // insertion.
                continue;
            }

            if (isAfterRebalanceOrValueUpdate(c, ctx)) {
                continue;
            }

            // AT THIS POINT EITHER (in all cases context is updated):
            // (1) Key wasn't found (key and value not valid)
            // (2) Key was found and it's value is deleted/invalid (key valid value invalid)
            if (!ctx.isKeyValid()) {
                if (!allocateAndLinkEntry(c, ctx, key, false)) {
                    continue; // allocation wasn't successfull and resulted in rebalance - retry
                }
            }

            c.allocateValue(ctx, value, false); // write value in place

            if (!c.publish()) {
                c.releaseNewValue(ctx);
                rebalance(c);
                continue;
            }

            if (c.linkValue(ctx) != ValueUtils.ValueResult.TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return null; // null can be returned only in zero-copy case
            }
        }
        throw new RuntimeException("put failed: reached retry limit (1024).");
    }

    // put the value assosiated with the key, only if key didn't exist
    // returned results describes whether the value was inserted or not
    Result putIfAbsent(K key, V value, OakTransformer<V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);
            // If exists a matching value reference for the given key, and it isn't marked deleted,
            // organize the return value: false for ZC, and old value deserialization for non-ZC
            if (ctx.isValueValid()) {
                if (transformer == null) {
                    return ctx.result.withFlag(ValueUtils.ValueResult.FALSE);
                }
                Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
                if (res.operationResult == ValueUtils.ValueResult.TRUE) {
                    return res;
                }
                continue;
            }

            if (isAfterRebalanceOrValueUpdate(c, ctx)) {
                continue;
            }

            // AT THIS POINT EITHER (in all cases context is updated):
            // (1) Key wasn't found (key and value not valid)
            // (2) Key was found and it's value is deleted/invalid (key valid value invalid)
            if (!ctx.isKeyValid()) {
                if (!allocateAndLinkEntry(c, ctx, key, true)) {
                    // allocation wasn't successful and resulted in rebalance,
                    // or retry is needed for other reason - retry
                    continue;
                }
            }

            c.allocateValue(ctx, value, false); // write value in place

            if (!c.publish()) {
                c.releaseNewValue(ctx);
                rebalance(c);
                continue;
            }

            if (c.linkValue(ctx) != ValueUtils.ValueResult.TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return ctx.result.withFlag(ValueUtils.ValueResult.TRUE);
            }
        }

        throw new RuntimeException("putIfAbsent failed: reached retry limit (1024).");
    }

    // if key didn't exist, put the value to be associated with the key
    // otherwise perform compute on the existing value
    // return false if compute happened, true if put happened
    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakScopedWriteBuffer> computer) {
        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);
            // If there is a matching value reference for the given key, and it is not marked as deleted,
            // then apply compute on the existing value
            if (ctx.isValueValid()) {
                ValueUtils.ValueResult res = valueOperator.compute(ctx.value, computer);
                if (res == ValueUtils.ValueResult.TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already found as deleted, continue to allocate a new value slice
                    return false;
                } else if (res == ValueUtils.ValueResult.RETRY) {
                    continue;
                }
            }

            if (isAfterRebalanceOrValueUpdate(c, ctx)) {
                continue;
            }

            // AT THIS POINT EITHER (in all cases context is updated):
            // (1) Key wasn't found (key and value not valid)
            // (2) Key was found and it's value is deleted/invalid (key valid value invalid)
            if (!ctx.isKeyValid()) {
                if (!allocateAndLinkEntry(c, ctx, key, false)) {
                    continue;
                }
            }

            c.allocateValue(ctx, value, false); // write value in place

            if (!c.publish()) {
                c.releaseNewValue(ctx);
                rebalance(c);
                continue;
            }

            if (c.linkValue(ctx) != ValueUtils.ValueResult.TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return true;
            }
        }

        throw new RuntimeException("putIfAbsentComputeIfPresent failed: reached retry limit (1024).");
    }

    // if key exists, remove the key-value mapping from the map
    Result remove(K key, V oldValue, OakTransformer<V> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        // when logicallyDeleted is true, it means we have marked the value as deleted.
        // Note that the entry will remain linked until rebalance happens.
        boolean logicallyDeleted = false;
        V v = null;

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);

            if (!ctx.isKeyValid()) {
                // There is no such key. If we did logical deletion and someone else did the physical deletion,
                // then the old value is saved in v. Otherwise v is (correctly) null
                return transformer == null ? ctx.result.withFlag(logicallyDeleted) : ctx.result.withValue(v);
            } else if (!ctx.isValueValid()) {
                // There is such a key, but the value is invalid,
                // either deleted (maybe only off-heap) or not yet allocated
                if (!finalizeDeletion(c, ctx)) {
                    // finalize deletion returns false, meaning no rebalance was requested
                    // and there was an attempt to finalize deletion
                    return transformer == null ? ctx.result.withFlag(logicallyDeleted) : ctx.result.withValue(v);
                }
                continue;
            }

            // AT THIS POINT Key wasn't found (key and value not valid) and context is updated
            if (logicallyDeleted) {
                // This is the case where we logically deleted this entry (marked the value off-heap as deleted),
                // but someone helped and (marked the value reference as deleted) and reused the entry
                // before we marked the value reference as deleted. We have the previous value saved in v.
                return transformer == null ? ctx.result.withFlag(ValueUtils.ValueResult.TRUE) : ctx.result.withValue(v);
            } else {
                Result removeResult = valueOperator.remove(ctx, oldValue, transformer);
                if (removeResult.operationResult == ValueUtils.ValueResult.FALSE) {
                    // we didn't succeed to remove the value: it didn't contain oldValue, or was already marked
                    // as deleted by someone else)
                    return ctx.result.withFlag(ValueUtils.ValueResult.FALSE);
                } else if (removeResult.operationResult == ValueUtils.ValueResult.RETRY) {
                    continue;
                }
                // we have marked this value as deleted (successful remove)
                logicallyDeleted = true;
                v = (V) removeResult.value;
            }

            // AT THIS POINT value was marked deleted off-heap by this thread,
            // continue to set the entry's value reference as deleted
            assert ctx.entryIndex != EntryArray.INVALID_ENTRY_INDEX;
            assert ctx.isValueValid();
            ctx.entryState = EntryArray.EntryState.DELETED_NOT_FINALIZED;

            if (inTheMiddleOfRebalance(c)) {
                continue;
            }

            // If finalize deletion returns true, meaning rebalance was done and there was NO
            // attempt to finalize deletion. There is going the help anyway, by next rebalance
            // or updater. Thus it is OK not to restart, the linearization point of logical deletion
            // is owned by this thread anyway and old value is kept in v.
            finalizeDeletion(c, ctx); // includes publish/unpublish
            return transformer == null ?
                ctx.result.withFlag(ValueUtils.ValueResult.TRUE) : ctx.result.withValue(v);
        }

        throw new RuntimeException("remove failed: reached retry limit (1024).");
    }

    // the zero-copy version of get
    OakUnscopedBuffer get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();
        OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
        c.lookUp(ctx, key);
        if (!ctx.isValueValid()) {
            return null;
        }
        return getValueUnscopedBuffer(ctx);

    }

    // if key with a valid value exists in the map, apply compute function on the value
    // return true if compute did happen
    boolean computeIfPresent(K key, Consumer<OakScopedWriteBuffer> computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);
            if (ctx.isValueValid()) {
                ValueUtils.ValueResult res = valueOperator.compute(ctx.value, computer);
                if (res == ValueUtils.ValueResult.TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already marked as deleted, continue to construct another slice
                    return true;
                } else if (res == ValueUtils.ValueResult.RETRY) {
                    continue;
                }
            }
            return false;
        }

        throw new RuntimeException("computeIfPresent failed: reached retry limit (1024).");
    }

    /**
     * Used when value of a key was possibly moved and we try to search for the given key
     * through the OakMap again.
     *
     * @param ctx The context key should be initialized with the key to refresh, and the context value
     *            will be updated with the refreshed value.
     * @reutrn true if the refresh was successful.
     */
    @Override
    boolean refreshValuePosition(ThreadContext ctx) {
        K deserializedKey = keySerializer.deserialize(ctx.key);
        OrderedChunk<K, V> c = findChunk(deserializedKey); // find orderedChunk matching key
        c.lookUp(ctx, deserializedKey);
        return ctx.isValueValid();
    }

    private <T> T getValueTransformation(OakScopedReadBuffer key, OakTransformer<T> transformer) {
        K deserializedKey = keySerializer.deserialize(key);
        return getValueTransformation(deserializedKey, transformer);
    }

    // the non-ZC variation of the get
    <T> T getValueTransformation(K key, OakTransformer<T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }

            Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
            if (res.operationResult == ValueUtils.ValueResult.RETRY) {
                continue;
            }
            return (T) res.value;
        }

        throw new RuntimeException("getValueTransformation failed: reached retry limit (1024).");
    }

    <T> T getKeyTransformation(K key, OakTransformer<T> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadContext();
        OrderedChunk<K, V> c = findChunk(key);
        c.lookUp(ctx, key);
        if (!ctx.isValueValid()) {
            return null;
        }
        return transformer.apply(ctx.key);
    }

    OakUnscopedBuffer getMinKey() {
        OrderedChunk<K, V> c = skiplist.firstEntry().getValue();
        ThreadContext ctx = getThreadContext();
        boolean isAllocated = c.readMinKey(ctx.key);
        return isAllocated ? getKeyUnscopedBuffer(ctx) : null;
    }

    <T> T getMinKeyTransformation(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        OrderedChunk<K, V> c = skiplist.firstEntry().getValue();
        ThreadContext ctx = getThreadContext();
        boolean isAllocated = c.readMinKey(ctx.tempKey);
        return isAllocated ? transformer.apply(ctx.tempKey) : null;
    }

    OakUnscopedBuffer getMaxKey() {
        OrderedChunk<K, V> c = skiplist.lastEntry().getValue();
        OrderedChunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in
        // the next orderedChunk we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }

        ThreadContext ctx = getThreadContext();
        boolean isAllocated = c.readMaxKey(ctx.key);
        return isAllocated ? getKeyUnscopedBuffer(ctx) : null;
    }

    <T> T getMaxKeyTransformation(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        OrderedChunk<K, V> c = skiplist.lastEntry().getValue();
        OrderedChunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in
        // the next orderedChunk we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }
        ThreadContext ctx = getThreadContext();
        boolean isAllocated = c.readMaxKey(ctx.tempKey);
        return isAllocated ? transformer.apply(ctx.tempKey) : null;
    }

    // encapsulates finding of the orderedChunk in the skip list and later orderedChunk list traversal
    private OrderedChunk<K, V> findChunk(K key) {
        OrderedChunk<K, V> c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);
        return c;
    }

    V replace(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }

            // will return null if the value is deleted
            Result result = valueOperator.exchange(c, ctx, value, valueDeserializeTransformer, valueSerializer);
            if (result.operationResult != ValueUtils.ValueResult.RETRY) {
                return (V) result.value;
            }
            // it might be that this chunk is proceeding with rebalance -> help
            helpRebalanceIfInProgress(c);
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    boolean replace(K key, V oldValue, V newValue, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            OrderedChunk<K, V> c = findChunk(key); // find orderedChunk matching key
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return false;
            }

            ValueUtils.ValueResult res = valueOperator.compareExchange(c, ctx, oldValue, newValue,
                    valueDeserializeTransformer, valueSerializer);
            if (res == ValueUtils.ValueResult.RETRY) {
                // it might be that this chunk is proceeding with rebalance -> help
                helpRebalanceIfInProgress(c);
                continue;
            }
            return res == ValueUtils.ValueResult.TRUE;
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    Map.Entry<K, V> lowerEntry(K key) {
        Map.Entry<Object, OrderedChunk<K, V>> lowerChunkEntry = skiplist.lowerEntry(key);
        if (lowerChunkEntry == null) {
            /* we were looking for the minimal key */
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }

        ThreadContext ctx = getThreadContext();

        OrderedChunk<K, V> c = lowerChunkEntry.getValue();
        /* Iterate orderedChunk to find prev(key), no upper limit */
        OrderedChunk.AscendingIter chunkIter = c.ascendingIter(ctx, null, false, null);
        int prevIndex = chunkIter.next(ctx);

        while (chunkIter.hasNext()) {
            int nextIndex = chunkIter.next(ctx);
            if (c.compareKeyAndEntryIndex(ctx.tempKey, key, nextIndex) <= 0) {
                break;
            }
            prevIndex = nextIndex;
        }

        /* Edge case: we're looking for the lowest key in the map and it's still greater than minkey
            (in which  case prevKey == key) */
        if (c.compareKeyAndEntryIndex(ctx.tempKey, key, prevIndex) == 0) {
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }
        // ctx.tempKey was updated with prevIndex key as a side effect of compareKeyAndEntryIndex()
        K keyDeserialized = keySerializer.deserialize(ctx.tempKey);

        // get value associated with this (prev) key
        boolean isAllocated = c.readValueFromEntryIndex(ctx.value, prevIndex);
        if (!isAllocated) { // value reference was invalid, try again
            return lowerEntry(key);
        }
        Result valueDeserialized = valueOperator.transform(ctx.result, ctx.value,
                valueSerializer::deserialize);
        if (valueDeserialized.operationResult != ValueUtils.ValueResult.TRUE) {
            return lowerEntry(key);
        }
        return new AbstractMap.SimpleImmutableEntry<>(keyDeserialized, (V) valueDeserialized.value);
    }

    /*-------------- Iterators --------------*/

    private static final class IteratorState<K, V> {

        private OrderedChunk<K, V> orderedChunk;
        private OrderedChunk.ChunkIter chunkIter;
        private int index;

        public void set(OrderedChunk<K, V> orderedChunk, OrderedChunk.ChunkIter chunkIter, int index) {
            this.orderedChunk = orderedChunk;
            this.chunkIter = chunkIter;
            this.index = index;
        }

        private IteratorState(
            OrderedChunk<K, V> nextOrderedChunk, OrderedChunk.ChunkIter nextChunkIter, int nextIndex) {

            this.orderedChunk = nextOrderedChunk;
            this.chunkIter = nextChunkIter;
            this.index = nextIndex;
        }

        OrderedChunk<K, V> getOrderedChunk() {
            return orderedChunk;
        }

        OrderedChunk.ChunkIter getChunkIter() {
            return chunkIter;
        }

        public int getIndex() {
            return index;
        }

        static <K, V> IteratorState<K, V> newInstance(
            OrderedChunk<K, V> nextOrderedChunk, OrderedChunk.ChunkIter nextChunkIter) {

            return new IteratorState<>(nextOrderedChunk, nextChunkIter, OrderedChunk.NONE_NEXT);
        }

    }

    void validateBoundariesOrder(K fromKey, K toKey) {
        if (fromKey != null && toKey != null && this.comparator.compareKeys(fromKey, toKey) > 0) {
            throw new IllegalArgumentException("inconsistent range");
        }
    }

    /**
     * Base of iterator classes:
     */
    abstract class Iter<T> implements Iterator<T> {

        private K lo;

        /* upper bound key, or null if to end */
        private K hi;
        /* inclusion flag for lo */
        private boolean loInclusive;
        /* inclusion flag for hi */
        private boolean hiInclusive;
        /* direction */
        private final boolean isDescending;

        /**
         * the next node to return from next();
         */
        private IteratorState<K, V> state;

        /**
         * An iterator cannot be accesses concurrently by multiple threads.
         * Thus, it is safe to have its own thread context.
         */
        protected ThreadContext ctx;

        /**
         * Initializes ascending iterator for entire range.
         */
        Iter(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            validateBoundariesOrder(lo, hi);

            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            this.isDescending = isDescending;
            this.ctx = new ThreadContext(keysMemoryManager, valuesMemoryManager);
            initState(isDescending, lo, loInclusive, hi, hiInclusive);

        }

        private boolean tooLow(OakScopedReadBuffer key) {
            if (lo == null) {
                return false;
            }
            int c = comparator.compareKeyAndSerializedKey(lo, key);
            return c > 0 || (c == 0 && !loInclusive);
        }

        private boolean tooHigh(OakScopedReadBuffer key) {
            if (hi == null) {
                return false;
            }
            int c = comparator.compareKeyAndSerializedKey(hi, key);
            return c < 0 || (c == 0 && !hiInclusive);
        }


        private boolean inBounds(OakScopedReadBuffer key) {
            if (!isDescending) {
                return !tooHigh(key);
            } else {
                return !tooLow(key);
            }
        }

        public final boolean hasNext() {
            return (state != null);
        }

        private void initAfterRebalance() {
            //TODO - refactor to use OakReadBuffer without deserializing.
            state.getOrderedChunk().readKeyFromEntryIndex(ctx.tempKey, state.getIndex());
            K nextKey = keySerializer.deserialize(ctx.tempKey);

            if (isDescending) {
                hiInclusive = true;
                hi = nextKey;
            } else {
                loInclusive = true;
                lo = nextKey;
            }

            // Update the state to point to last returned key.
            initState(isDescending, lo, loInclusive, hi, hiInclusive);
        }


        // the actual next()
        public abstract T next();

        /**
         * Advances next to higher entry.
         * Return previous index
         *
         * @return The first long is the key's reference, the integer is the value's version and the second long is
         * the value's reference. If {@code needsValue == false}, then the value of the map entry is {@code null}.
         */
        void advance(boolean needsValue) {
            boolean validState = false;

            while (!validState) {
                if (state == null) {
                    throw new NoSuchElementException();
                }

                final OrderedChunk<K, V> c = state.getOrderedChunk();
                if (c.state() == BasicChunk.State.RELEASED) {
                    initAfterRebalance();
                    continue;
                }

                final int curIndex = state.getIndex();

                // build the entry context that sets key references and does not check for value validity.
                ctx.initEntryContext(curIndex);

                if (!state.chunkIter.isBoundCheckNeeded()) {
                    c.readKey(ctx);
                } else {
                    // If we checked the boundary, than we already read the current key into ctx.tempKey
                    ctx.key.copyFrom(ctx.tempKey);
                }
                validState = ctx.isKeyValid();
                assert validState;

                if (needsValue) {
                    // Set value references and checks for value validity.
                    // if value is deleted ctx.entryState is going to be invalid
                    c.readValue(ctx);
                    validState = ctx.isValueValid();
                }

                advanceState();
            }
        }

        /**
         * Advances next to the next entry without creating a ByteBuffer for the key.
         * Return previous index
         */
        void advanceStream(UnscopedBuffer<KeyBuffer> key, UnscopedBuffer<ValueBuffer> value) {
            assert key != null || value != null;

            boolean validState = false;

            while (!validState) {
                if (state == null) {
                    throw new NoSuchElementException();
                }

                final OrderedChunk<K, V> c = state.getOrderedChunk();
                if (c.state() == BasicChunk.State.RELEASED) {
                    initAfterRebalance();
                    continue;
                }

                final int curIndex = state.getIndex();

                if (key != null) {
                    if (!state.chunkIter.isBoundCheckNeeded()) {
                        validState = c.readKeyFromEntryIndex(key.getInternalScopedReadBuffer(), curIndex);
                        assert validState;
                    } else {
                        // If we checked the boundary, than we already read the current key into ctx.tempKey
                        key.copyFrom(ctx.tempKey);
                        validState = true;
                    }
                }

                if (value != null) {
                    // If the current value is deleted, then advance and try again
                    validState = c.readValueFromEntryIndex(value.getInternalScopedReadBuffer(), curIndex);
                }

                advanceState();
            }
        }

        private void initState(boolean isDescending, K lowerBound, boolean lowerInclusive,
                               K upperBound, boolean upperInclusive) {

            OrderedChunk.ChunkIter nextChunkIter;
            OrderedChunk<K, V> nextOrderedChunk;

            if (!isDescending) {
                if (lowerBound != null) {
                    nextOrderedChunk = skiplist.floorEntry(lowerBound).getValue();
                } else {
                    nextOrderedChunk = skiplist.firstEntry().getValue();
                    // need to iterate from the beginning of the orderedChunk till the end
                }
                if (nextOrderedChunk != null) {
                    OakScopedReadBuffer upperBoundKeyForChunk = getNextChunkMinKey(nextOrderedChunk);
                    nextChunkIter = lowerBound != null ?
                        nextOrderedChunk.ascendingIter(ctx, lowerBound, lowerInclusive, upperBound, upperInclusive,
                            upperBoundKeyForChunk)
                        : nextOrderedChunk
                        .ascendingIter(ctx, upperBound, upperInclusive, upperBoundKeyForChunk);
                } else {
                    state = null;
                    return;
                }
            } else {
                nextOrderedChunk = upperBound != null ? skiplist.floorEntry(upperBound).getValue()
                        : skiplist.lastEntry().getValue();
                if (nextOrderedChunk != null) {
                    nextChunkIter = upperBound != null ?
                            nextOrderedChunk
                                .descendingIter(ctx, upperBound, upperInclusive, lowerBound, lowerInclusive)
                        : nextOrderedChunk.descendingIter(ctx, lowerBound, lowerInclusive);
                } else {
                    state = null;
                    return;
                }
            }

            //Init state, not valid yet, must move forward
            state = IteratorState.newInstance(nextOrderedChunk, nextChunkIter);
            advanceState();
        }

        private OrderedChunk<K, V> getNextChunk(OrderedChunk<K, V> current) {
            if (!isDescending) {
                return current.next.getReference();
            } else {
                Map.Entry<Object, OrderedChunk<K, V>> entry = skiplist.lowerEntry(current.minKey);
                if (entry == null) {
                    return null;
                } else {
                    return entry.getValue();
                }
            }
        }

        private OrderedChunk.ChunkIter getChunkIter(OrderedChunk<K, V> current) {
            if (!isDescending) {
                OakScopedReadBuffer upperBoundKeyForChunk = getNextChunkMinKey(current);
                return current.ascendingIter(ctx, hi, hiInclusive, upperBoundKeyForChunk);
            } else {
                return current.descendingIter(ctx, lo, loInclusive);
            }
        }

        private OakScopedReadBuffer getNextChunkMinKey(OrderedChunk<K, V> c) {
            OakScopedReadBuffer upperBoundKeyForChunk = null;
            if (hi != null) {
                // need to check upper bound for this ascending scan,
                // but does the next orderedChunk includes the upper bound?
                OrderedChunk nextNextOrderedChunk = c.next.getReference();
                // checking the min key of the next to next orderedChunk, in order to avoid search
                // for the maximal key on next orderedChunk. The minKey of the next
                // orderedChunk is higher than the max key of the current orderedChunk, therefore it can
                // only be unnecessary check for boundaries, but not correctness fault.
                if (nextNextOrderedChunk != null) {
                    upperBoundKeyForChunk = nextNextOrderedChunk.minKey;
                }
            }
            return upperBoundKeyForChunk;
        }

        private void advanceState() {

            OrderedChunk<K, V> orderedChunk = state.getOrderedChunk();
            OrderedChunk.ChunkIter chunkIter = state.getChunkIter();

            while (!chunkIter.hasNext()) { // chunks can have only removed keys
                orderedChunk = getNextChunk(orderedChunk);
                if (orderedChunk == null) {
                    //End of iteration
                    state = null;
                    return;
                }
                chunkIter = getChunkIter(orderedChunk);
            }

            int nextIndex = chunkIter.next(ctx);
            state.set(orderedChunk, chunkIter, nextIndex);

            // The boundary check is costly and need to be performed only when required,
            // meaning not on the full scan.
            if (chunkIter.isBoundCheckNeeded()) {
                orderedChunk.readKeyFromEntryIndex(ctx.tempKey, nextIndex);
                if (!inBounds(ctx.tempKey)) {
                    state = null;
                }
            }
        }
    }

    class ValueIterator extends Iter<OakUnscopedBuffer> {

        private final InternalOakMap<K, V> internalOakMap;

        ValueIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        @Override
        public OakUnscopedBuffer next() {
            advance(true);
            return getValueUnscopedBuffer(ctx);
        }
    }

    class ValueStreamIterator extends Iter<OakUnscopedBuffer> {

        private final UnscopedBuffer<ValueBuffer> value =
                new UnscopedBuffer<>(new ValueBuffer(valuesMemoryManager.getEmptySlice()));

        ValueStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakUnscopedBuffer next() {
            advanceStream(null, value);
            return value;
        }
    }

    class ValueTransformIterator<T> extends Iter<T> {

        final OakTransformer<T> transformer;

        ValueTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               OakTransformer<T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            advance(true);
            Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
            // If this value is deleted, try the next one
            if (res.operationResult == ValueUtils.ValueResult.FALSE) {
                return next();
            } else if (res.operationResult == ValueUtils.ValueResult.RETRY) {
                // if the value was moved, fetch it from the
                T result = getValueTransformation(ctx.key, transformer);
                if (result == null) {
                    // the value was deleted, try the next one
                    return next();
                }
                return result;
            }
            return (T) res.value;
        }
    }

    class EntryIterator extends Iter<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> {

        private final InternalOakMap<K, V> internalOakMap;

        EntryIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        public Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> next() {
            advance(true);
            return new AbstractMap.SimpleImmutableEntry<>(getKeyUnscopedBuffer(ctx), getValueUnscopedBuffer(ctx));
        }
    }

    class EntryStreamIterator extends Iter<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>>
            implements Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> {

        private final UnscopedBuffer<KeyBuffer> key =
            new UnscopedBuffer<>(new KeyBuffer(keysMemoryManager.getEmptySlice()));
        private final UnscopedBuffer<ValueBuffer> value =
                new UnscopedBuffer<>(new ValueBuffer(valuesMemoryManager.getEmptySlice()));

        EntryStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        public Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> next() {
            advanceStream(key, value);
            return this;
        }

        @Override
        public OakUnscopedBuffer getKey() {
            return key;
        }

        @Override
        public OakUnscopedBuffer getValue() {
            return value;
        }

        @Override
        public OakUnscopedBuffer setValue(OakUnscopedBuffer value) {
            throw new UnsupportedOperationException();
        }
    }

    class EntryTransformIterator<T> extends Iter<T> {

        final Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>, T> transformer;

        EntryTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            assert (transformer != null);
            this.transformer = transformer;
        }

        public T next() {
            advance(true);
            ValueUtils.ValueResult res = ctx.value.s.lockRead();
            if (res == ValueUtils.ValueResult.FALSE) {
                return next();
            } else if (res == ValueUtils.ValueResult.RETRY) {
                do {
                    boolean isSuccessful = refreshValuePosition(ctx);
                    if (!isSuccessful) {
                        return next();
                    }
                    res = ctx.value.s.lockRead();
                } while (res != ValueUtils.ValueResult.TRUE);
            }

            Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer> entry =
                    new AbstractMap.SimpleEntry<>(ctx.key, ctx.value);

            T transformation = transformer.apply(entry);
            ctx.value.s.unlockRead();
            return transformation;
        }
    }

    // May return deleted keys
    class KeyIterator extends Iter<OakUnscopedBuffer> {

        KeyIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakUnscopedBuffer next() {
            advance(false);
            return getKeyUnscopedBuffer(ctx);

        }
    }

    public class KeyStreamIterator extends Iter<OakUnscopedBuffer> {

        private final UnscopedBuffer<KeyBuffer> key
            = new UnscopedBuffer<>(new KeyBuffer(keysMemoryManager.getEmptySlice()));

        KeyStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakUnscopedBuffer next() {
            advanceStream(key, null);
            return key;
        }
    }

    class KeyTransformIterator<T> extends Iter<T> {

        final OakTransformer<T> transformer;

        KeyTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                             OakTransformer<T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            advance(false);
            return transformer.apply(ctx.key);
        }
    }

    // Factory methods for iterators

    Iterator<OakUnscopedBuffer> valuesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                         boolean isDescending) {
        return new ValueIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesBufferViewIterator(K lo, boolean loInclusive,
                                                                                        K hi, boolean hiInclusive,
                                                                                        boolean isDescending) {
        return new EntryIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<OakUnscopedBuffer> keysBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                       boolean isDescending) {
        return new KeyIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakUnscopedBuffer> valuesStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                     boolean isDescending) {
        return new ValueStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entriesStreamIterator(K lo, boolean loInclusive,
                                                                                    K hi, boolean hiInclusive,
                                                                                    boolean isDescending) {
        return new EntryStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakUnscopedBuffer> keysStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                   boolean isDescending) {
        return new KeyStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    <T> Iterator<T> valuesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                            boolean isDescending, OakTransformer<T> transformer) {
        return new ValueTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> entriesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                             boolean isDescending,
                                             Function<Map.Entry<OakScopedReadBuffer, OakScopedReadBuffer>,
                                                     T> transformer) {
        return new EntryTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> keysTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                                          OakTransformer<T> transformer) {
        return new KeyTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

}
