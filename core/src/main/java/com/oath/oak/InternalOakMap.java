/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;


import java.nio.ByteBuffer;
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

class InternalOakMap<K, V> {

    /*-------------- Members --------------*/

    final ConcurrentSkipListMap<Object, Chunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
    private final AtomicReference<Chunk<K, V>> head;
    private final ByteBuffer minKey;
    private final Comparator<Object> comparator;
    private final MemoryManager memoryManager;
    private final AtomicInteger size;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    private final ThreadIndexCalculator threadIndexCalculator;
    // The reference count is used to count the upper objects wrapping this internal map:
    // OakMaps (including subMaps and Views) when all of the above are closed,
    // his map can be closed and memory released.
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    /*-------------- Constructors --------------*/

    /**
     * init with capacity = 2g
     */

    InternalOakMap(
            K minKey,
            OakSerializer<K> keySerializer,
            OakSerializer<V> valueSerializer,
            Comparator<Object> comparator,
            MemoryManager memoryManager,
            int chunkMaxItems,
            int chunkBytesPerItem,
            ThreadIndexCalculator threadIndexCalculator) {

        this.size = new AtomicInteger(0);
        this.memoryManager = memoryManager;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.comparator = comparator;

        this.minKey = ByteBuffer.allocate(this.keySerializer.calculateSize(minKey));
        this.minKey.position(0);
        this.keySerializer.serialize(minKey, this.minKey);

        this.skiplist = new ConcurrentSkipListMap<>(this.comparator);

        Chunk<K, V> head = new Chunk<K, V>(this.minKey, null, this.comparator, memoryManager, chunkMaxItems,
            this.size, keySerializer, valueSerializer);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);
        this.threadIndexCalculator = threadIndexCalculator;
    }

    /*-------------- Closable --------------*/

    /**
     * cleans off heap memory
     */
    void close() {
        int res = referenceCount.decrementAndGet();
        // once reference count is zeroed, the map meant to be deleted and should not be used.
        // reference count will never grow again
        if (res == 0) {
            memoryManager.close();
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

    /*-------------- size --------------*/

    /**
     * @return current off heap memory usage in bytes
     */
    long memorySize() {
        return memoryManager.allocated();
    }

    int entries() {
        return size.get();
    }

    /*-------------- Methods --------------*/

    /**
     * finds and returns the chunk where key should be located, starting from given chunk
     */
    private Chunk<K, V> iterateChunks(Chunk<K, V> c, Object key) {
        // find chunk following given chunk (next)
        Chunk<K, V> next = c.next.getReference();

        // since skiplist isn't updated atomically in split/compaction, our key might belong in the next chunk
        // we need to iterate the chunks until we find the correct one
        while ((next != null) && (comparator.compare(next.minKey, key) <= 0)) {
            c = next;
            next = c.next.getReference();
        }

        return c;
    }


    private boolean rebalance(Chunk<K, V> c) {

        if (c == null) {
            // TODO: is this ok?
            return false;
        }
        Rebalancer<K, V> rebalancer = new Rebalancer<>(c, comparator, true, memoryManager, keySerializer,
                valueSerializer, threadIndexCalculator);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        boolean result = rebalancer.createNewChunks(); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk<K, V>> newChunks = rebalancer.getNewChunks();
        List<Chunk<K, V>> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        engaged.forEach(Chunk::release);

        return result;
    }

    private void checkRebalance(Chunk<K, V> c) {
        if (c.shouldRebalance()) {
            rebalance(c);
        }
    }

    private void connectToChunkList(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {

        updateLastChild(engaged, children);
        int countIterations = 0;
        Chunk<K, V> firstEngaged = engaged.get(0);

        // replace in linked list - we now need to find previous chunk to our chunk
        // and CAS its next to point to c1, which is the same c1 for all threads who reached this point
        // since prev might be marked (in compact itself) - we need to repeat this until successful
        while (true) {
            countIterations++;
            assert (countIterations < 10000); // this loop is not supposed to be infinite

            // start with first chunk (i.e., head)
            Map.Entry<Object, Chunk<K, V>> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

            Chunk<K, V> prev = lowerEntry != null ? lowerEntry.getValue() : null;
            Chunk<K, V> curr = (prev != null) ? prev.next.getReference() : null;

            // if didn't succeed to find prev through the skiplist - start from the head
            if (prev == null || curr != firstEngaged) {
                prev = null;
                curr = skiplist.firstEntry().getValue();    // TODO we can store&update head for a little efficiency
                // iterate until found chunk or reached end of list
                while ((curr != firstEngaged) && (curr != null)) {
                    prev = curr;
                    curr = curr.next.getReference();
                }
            }

            // chunk is head, we need to "add it to the list" for linearization point
            if (curr == firstEngaged && prev == null) {
                this.head.compareAndSet(firstEngaged, children.get(0));
                break;
            }
            // chunk is not in list (someone else already updated list), so we're done with this part
            if ((curr == null) || (prev == null)) {
                //TODO Never reached
                break;
            }

            // if prev chunk is marked - it is deleted, need to help split it and then continue
            if (prev.next.isMarked()) {
                rebalance(prev);
                continue;
            }

            // try to CAS prev chunk's next - from chunk (that we split) into c1
            // c1 is the old chunk's replacement, and is already connected to c2
            // c2 is already connected to old chunk's next - so all we need to do is this replacement
            if ((prev.next.compareAndSet(firstEngaged, children.get(0), false, false)) ||
                    (!prev.next.isMarked())) {
                // if we're successful, or we failed but prev is not marked - so it means someone else was successful
                // then we're done with loop
                break;
            }
        }

    }

    private void updateLastChild(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {
        Chunk<K, V> lastEngaged = engaged.get(engaged.size() - 1);
        Chunk<K, V> nextToLast = lastEngaged.markAndGetNext(); // also marks last engaged chunk as deleted
        Chunk<K, V> lastChild = children.get(children.size() - 1);

        lastChild.next.compareAndSet(null, nextToLast, false, false);
    }

    private void updateIndexAndNormalize(List<Chunk<K, V>> engagedChunks, List<Chunk<K, V>> children) {
        Iterator<Chunk<K, V>> iterEngaged = engagedChunks.iterator();
        Iterator<Chunk<K, V>> iterChildren = children.iterator();

        Chunk<K, V> firstEngaged = iterEngaged.next();
        Chunk<K, V> firstChild = iterChildren.next();

        // need to make the new chunks available, before removing old chunks
        skiplist.replace(firstEngaged.minKey, firstEngaged, firstChild);

        // remove all old chunks from index.
        while (iterEngaged.hasNext()) {
            Chunk engagedToRemove = iterEngaged.next();
            skiplist.remove(engagedToRemove.minKey, engagedToRemove); // conditional remove is used
        }

        // now after removing old chunks we can start normalizing
        firstChild.normalize();

        // for simplicity -  naive lock implementation
        // can be implemented without locks using versions on next pointer in skiplist
        while (iterChildren.hasNext()) {
            Chunk<K, V> childToAdd;
            synchronized (childToAdd = iterChildren.next()) {
                if (childToAdd.state() == Chunk.State.INFANT) { // make sure it wasn't add before
                    skiplist.putIfAbsent(childToAdd.minKey, childToAdd);
                    childToAdd.normalize();
                }
                // has a built in fence, so no need to add one here
            }
        }
    }

    private boolean rebalanceRemove(Chunk<K, V> c) {
        //TODO YONIGO - is it ok?
        return rebalance(c);
    }

    // Returns old handle if someone helped before pointToValue happened, or null if
    private boolean finishAfterPublishing(Chunk.OpData opData, Chunk<K, V> c) {
        // set pointer to value
        boolean result = c.pointToValue(opData);
        c.unpublish();
        checkRebalance(c);
        return result;
    }

    /*-------------- OakMap Methods --------------*/

    V put(K key, V value, Function<ByteBuffer, V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.valueSlice != null) {
            V v = (transformer != null) ? ValueUtils.transform(lookUp.valueSlice, transformer) : null;
            //TODO: fix when resize is required
            ValueUtils.put(lookUp.valueSlice, value, valueSerializer, memoryManager);
            return v;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator());
            put(key, value, transformer);
            return null;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalance(c);
            put(key, value, transformer);
            return null;
        }

        int ei = -1;
        long oldStats = 0;
        if (lookUp != null) {
            ei = lookUp.entryIndex;
            assert ei > 0;
            oldStats = lookUp.valueStats;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                rebalance(c);
                put(key, value, transformer);
                return null;
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                ei = prevEi;
                oldStats = c.getValueStats(prevEi);
            }
        }

        long newValueStats = c.writeValueOffHeap(value); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT, ei, newValueStats, oldStats, null);

        // publish put
        if (!c.publish()) {
            rebalance(c);
            put(key, value, transformer);
            return null;
        }

        finishAfterPublishing(opData, c);

        return null;
    }

    Result<V> putIfAbsent(K key, V value, Function<ByteBuffer, V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.valueSlice != null) {
            if (transformer == null) return Result.withFlag(false);
            return Result.withValue(ValueUtils.transform(lookUp.valueSlice, transformer));
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator());
            return putIfAbsent(key, value, transformer);
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalance(c);
            return putIfAbsent(key, value, transformer);
        }


        int ei = -1;
        long oldStats = 0;
        if (lookUp != null) {
            ei = lookUp.entryIndex;
            assert ei > 0;
            oldStats = lookUp.valueStats;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                rebalance(c);
                return putIfAbsent(key, value, transformer);
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                oldStats = c.getValueStats(prevEi);
                if (oldStats != 0) {
                    if (transformer == null) return Result.withFlag(false);
                    return Result.withValue(ValueUtils.transform(c.buildValueSlice(oldStats), transformer));
                } else {
                    ei = prevEi;
                }
            }
        }

        long newValueStats = c.writeValueOffHeap(value); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT_IF_ABSENT, ei, newValueStats, oldStats, null);

        // publish put
        if (!c.publish()) {
            rebalance(c);
            return putIfAbsent(key, value, transformer);
        }

        boolean result = finishAfterPublishing(opData, c);

        if (transformer == null) return Result.withFlag(result);
        // TODO: This is for the non-ZC API
        // What to do? Force access for the old buffer?
//        return Result.withValue((!result) ? oldHandle.transform(transformer) : null);
        throw new UnsupportedOperationException();
    }


    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer) {

        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.valueSlice != null) {
            if (ValueUtils.compute(lookUp.valueSlice, computer)) {
                // compute was successful and handle wasn't found deleted; in case
                // this handle was already found as deleted, continue to construct another handle
                return false;
            }
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator());
            return putIfAbsentComputeIfPresent(key, value, computer);
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalance(c);
            return putIfAbsentComputeIfPresent(key, value, computer);
        }

        // we come here when no key was found, which can be in 3 cases:
        // 1. no entry in the linked list at all
        // 2. entry in the linked list, but handle is not attached
        // 3. entry in the linked list, handle attached, but handle is marked deleted
        int ei = -1;
        long oldStats = 0;
        if (lookUp != null) {
            ei = lookUp.entryIndex;
            assert ei > 0;
            oldStats = lookUp.valueStats;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                rebalance(c);
                return putIfAbsentComputeIfPresent(key, value, computer);
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                oldStats = c.getValueStats(prevEi);
                if (oldStats != 0) {
                    if (ValueUtils.compute(c.buildValueSlice(oldStats), computer)) {
                        // compute was successful and handle wasn't found deleted; in case
                        // this handle was already found as deleted, continue to construct another handle
                        return false;
                    }
                } else {
                    ei = prevEi;
                }
            }
        }

        long newValueStats = c.writeValueOffHeap(value); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.COMPUTE, ei, newValueStats, oldStats, computer);

        // publish put
        if (!c.publish()) {
            rebalance(c);
            return putIfAbsentComputeIfPresent(key, value, computer);
        }

        return finishAfterPublishing(opData, c);
    }

    V remove(K key, V oldValue, Function<ByteBuffer, V> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        boolean logical = true; // when logical is false, means we have marked the handle as deleted
        Slice prev = null;
        V v = null;

        while (true) {

            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp != null && logical) {
                prev = lookUp.valueSlice; // remember previous handle
            }
            if (!logical && lookUp != null && prev != lookUp.valueSlice) {
                return v;  // someone else used this entry
            }

            if (lookUp == null || lookUp.valueSlice == null) {
                return v; // there is no such key
            }

            if (logical) {
                // we have marked this handle as deleted (successful remove)
                V vv = (transformer != null) ? ValueUtils.transform(lookUp.valueSlice, transformer) : null;

                if (oldValue != null && !oldValue.equals(vv))
                    return null;

                if (!ValueUtils.remove(lookUp.valueSlice, memoryManager)) {
                    // we didn't succeed to remove the handle was marked as deleted already
                    return null;
                }
                v = vv;
            }

            // if chunk is frozen or infant, we can't update it (remove deleted key, set handle index to -1)
            // we need to help rebalancer first, then proceed
            Chunk.State state = c.state();
            if (state == Chunk.State.INFANT) {
                // the infant is already connected so rebalancer won't add this put
                rebalance(c.creator());
                logical = false;
                continue;
            }
            if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
                if (!rebalanceRemove(c)) {
                    logical = false;
                    continue;
                }
                return v;
            }


            assert lookUp.entryIndex > 0;
            assert lookUp.valueStats > 0;
            Chunk.OpData opData = new Chunk.OpData(Operation.REMOVE, lookUp.entryIndex, 0, lookUp.valueStats, null);

            // publish
            if (!c.publish()) {
                if (!rebalanceRemove(c)) {
                    logical = false;
                    continue;
                }
                return v;
            }

            finishAfterPublishing(opData, c);
        }
    }

    OakRBuffer get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null) {
            return null;
        }
        return new OakRValueBufferImpl(lookUp.valueSlice.getByteBuffer());
    }

    <T> T getValueTransformation(K key, Function<ByteBuffer, T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null) {
            return null;
        }

        return ValueUtils.transform(lookUp.valueSlice, transformer);

    }

    <T> T getKeyTransformation(K key, Function<ByteBuffer, T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null || lookUp.entryIndex == -1) {
            return null;
        }
        ByteBuffer serializedKey = c.readKey(lookUp.entryIndex).slice();
        return transformer.apply(serializedKey);
    }

    ByteBuffer getKey(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key);
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null || lookUp.entryIndex == -1) {
            return null;
        }
        return c.readKey(lookUp.entryIndex).slice();
    }

    ByteBuffer getMinKey() {
        Chunk<K, V> c = skiplist.firstEntry().getValue();
        return c.readMinKey().slice();
    }

    <T> T getMinKeyTransformation(Function<ByteBuffer, T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = skiplist.firstEntry().getValue();
        ByteBuffer serializedMinKey = c.readMinKey();

        return (serializedMinKey != null) ? transformer.apply(serializedMinKey) : null;
    }

    ByteBuffer getMaxKey() {
        Chunk<K, V> c = skiplist.lastEntry().getValue();
        Chunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }

        return c.readMaxKey().slice();
    }

    <T> T getMaxKeyTransformation(Function<ByteBuffer, T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = skiplist.lastEntry().getValue();
        Chunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }
        ByteBuffer serializedMaxKey = c.readMaxKey();

        return (serializedMaxKey != null) ? transformer.apply(serializedMaxKey) : null;
    }

    boolean computeIfPresent(K key, Consumer<OakWBuffer> computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null) return false;

        return ValueUtils.compute(lookUp.valueSlice, computer);
    }

    // encapsulates finding of the chunk in the skip list and later chunk list traversal
    private Chunk<K, V> findChunk(Object key) {
        Chunk<K, V> c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);
        return c;
    }

    V replace(K key, V value, Function<ByteBuffer, V> valueDeserializeTransformer) {
        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null)
            return null;

        //TODO: deadlocks here
        Function<ByteBuffer, V> replaceTransform = bb -> {
            // mutatingTransform guarantees that this is write-synchronous handle is not deleted
            V v = valueDeserializeTransformer.apply(bb);

            ValueUtils.put(lookUp.valueSlice, value, valueSerializer, memoryManager);

            return v;
        };

        // will return null if handle was deleted between prior lookup and the next call
        return ValueUtils.mutatingTransform(lookUp.valueSlice, replaceTransform);
    }

    boolean replace(K key, V oldValue, V newValue, Function<ByteBuffer, V> valueDeserializeTransformer) {
        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null)
            return false;

        Function<ByteBuffer, Boolean> replaceTransform = bb -> {
            // mutatingTransform guarantees that this is write-synchronous handle is not deleted
            V v = valueDeserializeTransformer.apply(bb);
            if (!v.equals(oldValue))
                return false;

            ValueUtils.put(lookUp.valueSlice, newValue, valueSerializer, memoryManager);

            return true;
        };

        // res can be null if handle was deleted between lookup and the next call
        Boolean res = ValueUtils.mutatingTransform(lookUp.valueSlice, replaceTransform);
        return (res != null) ? res : false;
    }

    Map.Entry<K, V> lowerEntry(K key) {
        Map.Entry<Object, Chunk<K, V>> lowerChunkEntry = skiplist.lowerEntry(key);
        if (lowerChunkEntry == null) {
            /* we were looking for the minimal key */
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }

        Chunk<K, V> c = lowerChunkEntry.getValue();
        /* Iterate chunk to find prev(key) */
        Chunk.AscendingIter chunkIter = c.ascendingIter();
        int prevIndex = chunkIter.next();

        while (chunkIter.hasNext()) {
            int nextIndex = chunkIter.next();
            int cmp = comparator.compare(c.readKey(nextIndex), key);
            if (cmp >= 0) {
                break;
            }
            prevIndex = nextIndex;
        }

        /* Edge case: we're looking for the lowest key in the map and it's still greater than minkey
            (in which  case prevKey == key) */
        ByteBuffer prevKey = c.readKey(prevIndex);
        if (comparator.compare(prevKey, key) == 0) {
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }

        // TODO: No lock?
        return new AbstractMap.SimpleImmutableEntry<>(
                keySerializer.deserialize(prevKey),
                valueSerializer.deserialize(ValueUtils.getActualValueBuffer(c.getValueSlice(prevIndex).getByteBuffer()).asReadOnlyBuffer()));
    }

    /*-------------- Iterators --------------*/


    private static class IteratorState<K, V> {

        private Chunk<K, V> chunk;
        private Chunk.ChunkIter chunkIter;
        private int index;

        public void set(Chunk<K, V> chunk, Chunk.ChunkIter chunkIter, int index) {
            this.chunk = chunk;
            this.chunkIter = chunkIter;
            this.index = index;
        }

        private IteratorState(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter, int nextIndex) {
            this.chunk = nextChunk;
            this.chunkIter = nextChunkIter;
            this.index = nextIndex;
        }

        Chunk<K, V> getChunk() {
            return chunk;
        }

        Chunk.ChunkIter getChunkIter() {
            return chunkIter;
        }

        public int getIndex() {
            return index;
        }


        public static <K, V> IteratorState<K, V> newInstance(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter) {
            return new IteratorState<>(nextChunk, nextChunkIter, Chunk.NONE);
        }

    }

    /**
     * Base of iterator classes:
     */
    abstract class Iter<T> implements Iterator<T> {

        private K lo;

        /**
         * upper bound key, or null if to end
         */
        private K hi;
        /**
         * inclusion flag for lo
         */
        private boolean loInclusive;
        /**
         * inclusion flag for hi
         */
        private boolean hiInclusive;
        /**
         * direction
         */
        private final boolean isDescending;

        /**
         * the next node to return from next();
         */
        private IteratorState<K, V> state;

        /**
         * Initializes ascending iterator for entire range.
         */
        Iter(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            if (lo != null && hi != null &&
                    comparator.compare(lo, hi) > 0)
                throw new IllegalArgumentException("inconsistent range");

            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            this.isDescending = isDescending;

            initState(isDescending, lo, loInclusive, hi, hiInclusive);

        }

        boolean tooLow(Object key) {
            int c;
            return (lo != null && ((c = comparator.compare(key, lo)) < 0 ||
                    (c == 0 && !loInclusive)));
        }

        boolean tooHigh(Object key) {
            int c;
            return (hi != null && ((c = comparator.compare(key, hi)) > 0 ||
                    (c == 0 && !hiInclusive)));
        }


        boolean inBounds(Object key) {
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
            //TODO - refactor to use ByeBuffer without deserializing.
            K nextKey = keySerializer.deserialize(state.getChunk().readKey(state.getIndex()).slice());

            if (isDescending) {
                hiInclusive = true;
                hi = nextKey;
            } else {
                loInclusive = true;
                lo = nextKey;
            }

            // Update the state to point to last returned key.
            initState(isDescending, lo, loInclusive, hi, hiInclusive);

            if (state == null) {
                throw new ConcurrentModificationException();
            }
        }


        // the actual next()
        abstract public T next();

        /**
         * Advances next to higher entry.
         * Return previous index
         */
        Map.Entry<ByteBuffer, Slice> advance() {

            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }

            ByteBuffer bb = state.getChunk().readKey(state.getIndex()).slice();
            Slice currentValue = state.getChunk().getValueSlice(state.getIndex());
            advanceState();
            return new AbstractMap.SimpleImmutableEntry<>(bb, currentValue);
        }

        /**
         * Advances next to the next entry without creating a ByteBuffer for the key.
         * Return previous index
         */
        Handle advanceStream(OakRKeyReference key, boolean keyOnly) {

            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }
            Handle currentHandle = null;
            if (key != null) {
              state.getChunk().setKeyRefer(state.getIndex(),key);
            }
            if (!keyOnly) {
              currentHandle = state.getChunk().getHandle(state.getIndex());
            }
            advanceState();
            return currentHandle;
        }

        private void initState(boolean isDescending, K lowerBound, boolean lowerInclusive,
                               K upperBound, boolean upperInclusive) {

            Chunk.ChunkIter nextChunkIter = null;
            Chunk<K, V> nextChunk;

            if (!isDescending) {
                if (lowerBound != null)
                    nextChunk = skiplist.floorEntry(lowerBound).getValue();
                else
                    nextChunk = skiplist.floorEntry(minKey).getValue();
                if (nextChunk != null) {
                    nextChunkIter = lowerBound != null ?
                            nextChunk.ascendingIter(lowerBound, lowerInclusive) : nextChunk.ascendingIter();
                } else {
                    state = null;
                    return;
                }
            } else {
                nextChunk = upperBound != null ? skiplist.floorEntry(upperBound).getValue()
                        : skiplist.lastEntry().getValue();
                if (nextChunk != null) {
                    nextChunkIter = upperBound != null ?
                            nextChunk.descendingIter(upperBound, upperInclusive) : nextChunk.descendingIter();
                } else {
                    state = null;
                    return;
                }
            }

            //Init state, not valid yet, must move forward
            state = IteratorState.newInstance(nextChunk, nextChunkIter);
            advanceState();
        }

        private Chunk<K, V> getNextChunk(Chunk<K, V> current) {
            if (!isDescending) {
                return current.next.getReference();
            } else {
                ByteBuffer serializedMinKey = current.minKey;
                Map.Entry<Object, Chunk<K, V>> entry = skiplist.lowerEntry(serializedMinKey);
                if (entry == null) {
                    return null;
                } else {
                    return entry.getValue();
                }
            }
        }

        private Chunk.ChunkIter getChunkIter(Chunk<K, V> current) {
            if (!isDescending) {
                return current.ascendingIter();
            } else {
                return current.descendingIter();
            }
        }

        private void advanceState() {

            Chunk<K, V> chunk = state.getChunk();
            Chunk.ChunkIter chunkIter = state.getChunkIter();

            while (!chunkIter.hasNext()) { // chunks can have only removed keys
                chunk = getNextChunk(chunk);
                if (chunk == null) {
                    //End of iteration
                    state = null;
                    return;
                }
                chunkIter = getChunkIter(chunk);
            }

            int nextIndex = chunkIter.next();
            state.set(chunk, chunkIter, nextIndex);

            // The boundary check is costly and need to be performed only when required,
            // meaning not on the full scan.
            // The check of the boundaries under condition is an optimization.
            if ((hi!=null && !isDescending) || (lo!=null && isDescending)) {
                ByteBuffer key = state.getChunk().readKey(state.getIndex());
                if (!inBounds(key)) {
                    state = null;
                    return;
                }
            }
        }
    }

    class ValueIterator extends Iter<OakRBuffer> {

        ValueIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {
            Slice valueSlice = advance().getValue();
            if (valueSlice == null)
                return null;

            return new OakRValueBufferImpl(valueSlice.getByteBuffer());
        }
    }

    class ValueStreamIterator extends Iter<OakRBuffer> {

        private OakRValueBufferImpl value = new OakRValueBufferImpl(null);

        ValueStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {
            Handle handle = advanceStream(null, false);
            if (handle == null) {
                return null;
            }
            value.setHandle(handle);
            return value;
        }
    }

    class ValueTransformIterator<T> extends Iter<T> {

        final Function<ByteBuffer, T> transformer;

        ValueTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               Function<ByteBuffer, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            Slice valueSlice = advance().getValue();
            if (valueSlice == null)
                return null;
            return ValueUtils.transform(valueSlice, transformer);
        }
    }

    class EntryIterator extends Iter<Map.Entry<OakRBuffer, OakRBuffer>> {

        EntryIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        public Map.Entry<OakRBuffer, OakRBuffer> next() {
            Map.Entry<ByteBuffer, Slice> pair = advance();
            if (pair.getValue() == null) {
                return null;
            }
            return new AbstractMap.SimpleImmutableEntry<>(
                    new OakRKeyBufferImpl(pair.getKey()),
                    new OakRValueBufferImpl(pair.getValue().getByteBuffer()));
        }
    }

    class EntryStreamIterator extends Iter<Map.Entry<OakRBuffer, OakRBuffer>> {

        private OakRKeyReference key = new OakRKeyReference(memoryManager);
        private OakRValueBufferImpl value = new OakRValueBufferImpl(null);

        EntryStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        public Map.Entry<OakRBuffer, OakRBuffer> next() {
            Handle handle = advanceStream(key, false);
            if (handle == null) {
                return null;
            }
            value.setHandle(handle);
            return new AbstractMap.SimpleImmutableEntry<>(key, value);
        }
    }

    class EntryTransformIterator<T> extends Iter<T> {

        final Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

        EntryTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            assert (transformer != null);
            this.transformer = transformer;
        }

        public T next() {

            Map.Entry<ByteBuffer, Slice> pair = advance();
            ByteBuffer serializedKey = pair.getKey();
            Slice valueSlice = pair.getValue();
            if (valueSlice == null) {
                return null;
            }
            if (!ValueUtils.lockRead(valueSlice))
                return null;
            ByteBuffer serializedValue = ValueUtils.getActualValueBuffer(valueSlice.getByteBuffer()).asReadOnlyBuffer();
            Map.Entry<ByteBuffer, ByteBuffer> entry = new AbstractMap.SimpleEntry<>(serializedKey, serializedValue);

            T transformation = transformer.apply(entry);
            ValueUtils.unlockRead(valueSlice);
            return transformation;
        }
    }

    class KeyIterator extends Iter<OakRBuffer> {

        KeyIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {

            Map.Entry<ByteBuffer, Slice> pair = advance();
            return new OakRKeyBufferImpl(pair.getKey());

        }
    }

    public class KeyStreamIterator extends Iter<OakRBuffer> {

        private OakRKeyReference key = new OakRKeyReference(memoryManager);

        KeyStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(null, loInclusive, null, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {
            advanceStream(key, true);
            return key;
        }
    }

    class KeyTransformIterator<T> extends Iter<T> {

        final Function<ByteBuffer, T> transformer;

        KeyTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                             Function<ByteBuffer, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            Map.Entry<ByteBuffer, Slice> pair = advance();
            ByteBuffer serializedKey = pair.getKey();
            return transformer.apply(serializedKey);
        }
    }

    // Factory methods for iterators

    Iterator<OakRBuffer> valuesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new ValueIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new EntryIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakRBuffer> keysBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new KeyIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakRBuffer> valuesStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new ValueStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new EntryStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakRBuffer> keysStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new KeyStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    <T> Iterator<T> valuesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, Function<ByteBuffer, T> transformer) {
        return new ValueTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> entriesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
        return new EntryTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> keysTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, Function<ByteBuffer, T> transformer) {
        return new KeyTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    static class Result<V> {
        final V value;
        final boolean flag;
        final boolean hasValue;

        private Result(V value, boolean flag, boolean hasValue) {
            this.value = value;
            this.flag = flag;
            this.hasValue = hasValue;

        }

        static <V> Result<V> withValue(V value) {
            return new Result<>(value, false, true);
        }

        static <V> Result<V> withFlag(boolean flag) {
            return new Result<>(null, flag, false);
        }

    }
}
