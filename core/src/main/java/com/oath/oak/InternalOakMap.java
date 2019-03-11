/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
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
import java.util.logging.Logger;
import java.util.AbstractMap;

class InternalOakMap<K, V> {

    /*-------------- Members --------------*/

    Logger log = Logger.getLogger(InternalOakMap.class.getName());
    final ConcurrentSkipListMap<Object, Chunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
    private final AtomicReference<Chunk<K, V>> head;
    private final ByteBuffer minKey;
    private final Comparator comparator;
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
            Comparator comparator,
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
                chunkBytesPerItem, this.size, keySerializer, valueSerializer, threadIndexCalculator);
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
        while(true) {
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

    int entries() { return size.get(); }

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



    private Rebalancer.RebalanceResult rebalance(Chunk<K, V> c, K key, V value, Consumer<ByteBuffer> computer, Operation op) {
        if (c == null) {
            return null;
        }
        Rebalancer rebalancer = new Rebalancer(c, comparator, true, memoryManager, keySerializer,
                valueSerializer, threadIndexCalculator);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        Rebalancer.RebalanceResult result = rebalancer.createNewChunks(); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk<K, V>> newChunks = rebalancer.getNewChunks();
        List<Chunk<K, V>> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        engaged.forEach(Chunk::release);

        if (result.isSuccess() && result.getHelpedOp() == Operation.PUT_IF_ABSENT) {
            size.incrementAndGet();
        }

        return result;
    }

    private void checkRebalance(Chunk c) {
        if (c.shouldRebalance()) {
            rebalance(c, null, null, null, Operation.NO_OP);
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
            assert (countIterations<10000); // this loop is not supposed to be infinite

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
                rebalance(prev, null, null, null, Operation.NO_OP);
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

        Chunk firstEngaged = iterEngaged.next();
        Chunk firstChild = iterChildren.next();

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
            Chunk childToAdd;
            synchronized (childToAdd = iterChildren.next()) {
                if (childToAdd.state() == Chunk.State.INFANT) { // make sure it wasn't add before
                    skiplist.putIfAbsent(childToAdd.minKey, childToAdd);
                    childToAdd.normalize();
                }
                // has a built in fence, so no need to add one here
            }
        }
    }


    /*-------------- OakMap Methods --------------*/

    void put(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        Chunk<K,V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null) {
            if (lookUp.handle.put(value, valueSerializer, memoryManager)) {
                c.getStatistics().incrementAddedCount();
                return;
            }

        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            put(key, value);
            return;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalance(c, null, null, null, Operation.NO_OP);
            put(key, value);
            return;
        }

        int hi = c.allocateHandle();
        int ei = c.allocateEntryAndKey(key);
        if (hi == -1 || ei == -1) {
            rebalance(c, null, null, null, Operation.NO_OP);
            put(key, value);
            return;
        }
        c.writeValue(hi, value);

        //attache handle to entry
        c.pointToValue(ei, hi);

        // publish put
        // just put some value so that rebalance waits for this thread to finish
        // If rebalancer gets to put an op in array then publish fails
        //TODO YONIGO - this op is stupid
        Chunk.OpData op = new Chunk.OpData(Operation.PUT, -1, -1, -1, null);
        if (!c.publish(op)) {
            c.getHandle(ei).remove(memoryManager);
            rebalance(c, null, null, null, Operation.NO_OP);
            put(key, value);
            return;
        }

        //attache entry to chunk list
        Chunk.LinkEntryResult linkResult = c.linkEntry(ei, true, key);
        if(!linkResult.isNewEntry()) {
            c.getHandle(ei).remove(memoryManager);
            if (c.getHandle(linkResult.getEi()).put(value, valueSerializer, memoryManager)) {
                c.getStatistics().incrementAddedCount();
            }
        } else {
            c.getStatistics().incrementAddedCount();
        }


        c.unpublish(op);

    }

    boolean putIfAbsent(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        Chunk<K,V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp<V> lookUp = c.lookUp(key);
        if (lookUp != null) {
            //TODO YONIGO TEST
            if(lookUp.handle.putIfAbsent(value, valueSerializer, memoryManager)) {
                c.getStatistics().incrementAddedCount();
                return true;
            }
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            return putIfAbsent(key, value);
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalance(c, null, null, null, Operation.NO_OP);
            return putIfAbsent(key, value);
        }

        int hi = c.allocateHandle();
        int ei = c.allocateEntryAndKey(key);
        if (hi == -1 || ei == -1) {
            rebalance(c, null, null, null, Operation.NO_OP);
            return putIfAbsent(key, value);
        }
        c.writeValue(hi, value);

        //attache handle to entry
        c.pointToValue(ei, hi);

        // publish put
        // just put some value so that rebalance waits for this thread to finish
        // If rebalancer gets to put an op in array then publish fails
        //TODO YONIGO - this op is stupid
        Chunk.OpData op = new Chunk.OpData(Operation.PUT_IF_ABSENT, -1, -1, -1, null);
        if (!c.publish(op)) {
            c.getHandle(ei).remove(memoryManager);
            rebalance(c, null, null, null, Operation.NO_OP);
            return putIfAbsent(key, value);
        }

        //attache entry to chunk list
        Chunk.LinkEntryResult linkResult = c.linkEntry(ei, true, key);
        if(!linkResult.isNewEntry()) {
            c.getHandle(ei).remove(memoryManager);
            boolean retVal = c.getHandle(linkResult.getEi()).putIfAbsent(value, valueSerializer, memoryManager);
            if (retVal) {
                c.getStatistics().incrementAddedCount();
            }
            c.unpublish(op);
            checkRebalance(c);
            return retVal;
        } else {
            c.getStatistics().incrementAddedCount();
            c.unpublish(op);
            checkRebalance(c);
            return true;
        }
    }

    //return true if added a new value
    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<ByteBuffer> computer) {
        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        Chunk<K,V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp<V> lookUp = c.lookUp(key);
        if (lookUp != null) {
            //maybe entry is deleted so must try:
            if (lookUp.handle.putIfAbsentComputeIfPresent(value, valueSerializer, computer, memoryManager)) {
                c.getStatistics().incrementAddedCount();
                return true;
            }
            return false;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            return putIfAbsentComputeIfPresent(key, value, computer);
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalance(c, null, null, null, Operation.NO_OP);
            return putIfAbsentComputeIfPresent(key, value, computer);
        }

        int hi = c.allocateHandle(); // TODO YONIGO - This handler is not released if during rebalance helpOp handler gets computed
        int ei = c.allocateEntryAndKey(key);
        if (hi == -1 || ei == -1) {
            rebalance(c, null, null, null, Operation.NO_OP);
            return putIfAbsentComputeIfPresent(key, value, computer);
        }
        c.writeValue(hi, value);

        //attache handle to entry
        c.pointToValue(ei, hi);

        // publish put
        // just put some value so that rebalance waits for this thread to finish
        // If rebalancer gets to put an op in array then publish fails
        //TODO YONIGO - this op is stupid
        Chunk.OpData op = new Chunk.OpData(Operation.PUT_IF_ABS_COMPUTE_IF_PRES, -1, -1, -1, null);
        if (!c.publish(op)) {
            c.getHandle(ei).remove(memoryManager);
            rebalance(c, null, null, null, Operation.NO_OP);
            return putIfAbsentComputeIfPresent(key, value, computer);
        }

        //attache entry to chunk list
        Chunk.LinkEntryResult linkResult = c.linkEntry(ei, true, key);
        if(!linkResult.isNewEntry()) {
            //another thread inserted this key so we compute
            c.getHandle(ei).remove(memoryManager);
            if (c.getHandle(linkResult.getEi()).putIfAbsentComputeIfPresent(value, valueSerializer, computer, memoryManager)) {
                c.getStatistics().incrementAddedCount();
                c.unpublish(op);
                return true;
            } else {
                c.unpublish(op);
                return false;
            }

        } else {
            c.getStatistics().incrementAddedCount();
            c.unpublish(op);
            return true;
        }
    }

    void remove(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        Chunk c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null) {
            if (lookUp.handle.remove(memoryManager)) {
                c.getStatistics().decrementAddedCount();
            }
        }
        checkRebalance(c);
    }

    OakRBuffer get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) {
            return null;
        }
        return new OakRValueBufferImpl(lookUp.handle);
    }

    <T> T getValueTransformation(K key, Function<ByteBuffer,T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) {
            return null;
        }

        T transformation = (T) lookUp.handle.transform(transformer);
        return transformation;

    }

    <T> T getKeyTransformation(K key, Function<ByteBuffer,T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null || lookUp.entryIndex == -1) {
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
        if (lookUp == null || lookUp.handle == null || lookUp.entryIndex == -1) {
            return null;
        }
        return c.readKey(lookUp.entryIndex).slice();
    }

    ByteBuffer getMinKey() {
        Chunk<K, V> c = skiplist.firstEntry().getValue();
        return c.readMinKey().slice();
    }

    <T> T getMinKeyTransformation(Function<ByteBuffer,T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = skiplist.firstEntry().getValue();
        ByteBuffer serializedMinKey = c.readMinKey();

        return transformer.apply(serializedMinKey);
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

    <T> T getMaxKeyTransformation(Function<ByteBuffer,T> transformer) {
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
        return transformer.apply(serializedMaxKey);
    }

    boolean computeIfPresent(K key, Consumer<ByteBuffer> computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        Chunk c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) return false;

        return lookUp.handle.compute(computer);
    }

    // encapsulates finding of the chunk in the skip list and later chunk list traversal
    private Chunk findChunk(Object key) {
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);
        return c;
    }

    /*-------------- Iterators --------------*/


    private static class IteratorState<K, V> {

        private Chunk<K, V> chunk;
        private Chunk.ChunkIter chunkIter;
        private int index;

        public void set(Chunk<K, V> chunk, Chunk.ChunkIter chunkIter, int index) {
            this.chunk = chunk;
            this.chunkIter=chunkIter;
            this.index = index;
        }

        private IteratorState(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter, int nextIndex) {
            this.chunk = nextChunk;
            this.chunkIter = nextChunkIter;
            this.index = nextIndex;
        }

        public Chunk<K, V> getChunk() {
            return chunk;
        }

        public Chunk.ChunkIter getChunkIter() {
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
    abstract class Iter<T> implements OakIterator<T> {

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
        private IteratorState<K,V> state;

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
            }
            else {
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
        Pair<ByteBuffer, Handle> advance() {

            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }

            ByteBuffer bb = state.getChunk().readKey(state.getIndex()).slice();
            Handle currentHandle = state.getChunk().getHandle(state.getIndex());
            advanceState();
            return new Pair<>(bb, currentHandle);
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

            ByteBuffer key = state.getChunk().readKey(state.getIndex());
            if (!inBounds(key)) {
                state = null;
                return;
            }
        }
    }

    class ValueIterator extends Iter<OakRBuffer> {

        ValueIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {
            Handle handle = advance().getValue();
            if (handle.isDeleted())
                return null;

            return new OakRValueBufferImpl(handle);
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
            Handle handle = advance().getValue();
            if (handle == null) {
                return null;
            }
            return (T)handle.transform(transformer);
        }
    }

    class EntryIterator extends Iter<Map.Entry<ByteBuffer, OakRBuffer>> {

        EntryIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        public Map.Entry<ByteBuffer, OakRBuffer> next() {
            Pair<ByteBuffer, Handle> pair = advance();
            if (pair.getValue().isDeleted()) {
                return null;
            }
            return new AbstractMap.SimpleImmutableEntry<>(pair.getKey(),
                    new OakRValueBufferImpl(pair.getValue()));
        }
    }

    class EntryTransformIterator<T> extends Iter<T> {

        final Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

        EntryTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            assert(transformer!=null);
            this.transformer = transformer;
        }

        public T next() {

            Pair<ByteBuffer, Handle> pair = advance();
            Handle handle = pair.getValue();
            ByteBuffer serializedKey = pair.getKey();
            if (handle == null) {
                return null;
            }
            handle.readLock();
            if (handle.isDeleted()) {
                handle.readUnLock();
                return null;
            }
            ByteBuffer serializedValue = handle.getSlicedReadOnlyByteBuffer();
            Map.Entry<ByteBuffer, ByteBuffer> entry = new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(serializedKey, serializedValue);

            T transformation = transformer.apply(entry);
            handle.readUnLock();
            return transformation;
        }
    }

    class KeyIterator extends Iter<ByteBuffer> {

        KeyIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public ByteBuffer next() {

            Pair<ByteBuffer, Handle> pair = advance();
            return pair.getKey();

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
            Pair<ByteBuffer, Handle> pair = advance();
            ByteBuffer serializedKey = pair.getKey();
            return transformer.apply(serializedKey);
        }
    }

    // Factory methods for iterators

    OakIterator<OakRBuffer> valuesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new ValueIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    OakIterator<Map.Entry<ByteBuffer, OakRBuffer>> entriesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new EntryIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    OakIterator<ByteBuffer> keysBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
        return new KeyIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    <T> OakIterator<T> valuesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, Function<ByteBuffer, T> transformer) {
        return new ValueTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> OakIterator<T> entriesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
        return new EntryTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> OakIterator<T> keysTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, Function<ByteBuffer, T> transformer) {
        return new KeyTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

}
