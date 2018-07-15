/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Logger;

public class OakMapOffHeapImpl<K, V> implements OakMap<K, V>, AutoCloseable {

    /*-------------- Members --------------*/

    Logger log = Logger.getLogger(OakMapOffHeapImpl.class.getName());
    final ConcurrentSkipListMap<Object, Chunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
    private final AtomicReference<Chunk<K, V>> head;
    private final ByteBuffer minKey;
    private final Comparator comparator;
    final OakMemoryManager memoryManager;
    private final HandleFactory handleFactory;
    private final ValueFactory valueFactory;
    private AtomicInteger size;
    private Serializer<K> keySerializer;
    private Deserializer<K> keyDeserializer;
    private SizeCalculator<K> keySizeCalculator;
    private Serializer<V> valueSerializer;
    private Deserializer<V> valueDeserializer;
    private SizeCalculator<V> valueSizeCalculator;

    /**
     * Lazily initialized descending key set
     */
    private OakMap descendingMap;

    /*-------------- Constructors --------------*/

    /**
     * init with capacity = 2g
     */
    public OakMapOffHeapImpl(
            K minKey,
            Serializer<K> keySerializer,
            Deserializer<K> keyDeserializer,
            SizeCalculator<K> keySizeCalculator,
            Serializer<V> valueSerializer,
            Deserializer<V> valueDeserializer,
            SizeCalculator<V> valueSizeCalculator,
            OakComparator<K, K> keysComparator,
            OakComparator<ByteBuffer, ByteBuffer> serializationsComparator,
            OakComparator<ByteBuffer, K> serializationAndKeyComparator,
            MemoryPool memoryPool,
            int chunkMaxItems,
            int chunkBytesPerItem) {

        this.size = new AtomicInteger(0);
        this.memoryManager = new OakMemoryManager(memoryPool);

        this.keySerializer = keySerializer;
        this.keyDeserializer = keyDeserializer;
        this.keySizeCalculator = keySizeCalculator;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
        this.valueSizeCalculator = valueSizeCalculator;

        this.comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                if (o1 instanceof ByteBuffer) {
                    if (o2 instanceof ByteBuffer) {
                        return serializationsComparator.compare((ByteBuffer) o1, (ByteBuffer) o2);
                    } else {
                        return serializationAndKeyComparator.compare((ByteBuffer) o1, (K) o2);
                    }
                } else {
                    if (o2 instanceof ByteBuffer) {
                        return (-1) * serializationAndKeyComparator.compare((ByteBuffer) o2, (K) o1);
                    } else {
                        return keysComparator.compare((K) o1, (K) o2);
                    }
                }
            }
        };

        this.minKey = ByteBuffer.allocate(this.keySizeCalculator.calculateSize(minKey));
        this.minKey.position(0);
        this.keySerializer.serialize(minKey, this.minKey);

        this.skiplist = new ConcurrentSkipListMap<>(this.comparator);
        Chunk<K, V> head = new Chunk<K, V>(this.minKey, null, comparator, memoryManager, chunkMaxItems,
                chunkBytesPerItem, this.size, keySerializer, keySizeCalculator, valueSerializer, valueSizeCalculator);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);

        this.descendingMap = null;
        this.handleFactory = new HandleFactory(true);
        this.valueFactory = new ValueFactory(true);
    }


    static int getThreadIndex() {
      // TODO use hash instead of modulo
      return (int) (Thread.currentThread().getId() % Chunk.MAX_THREADS);
    }

    /*-------------- Closable --------------*/

    /**
     * cleans off heap memory
     */
    @Override
    public void close() {
        memoryManager.pool.clean();
    }

    /*-------------- size --------------*/

    /**
     * @return current off heap memory usage in bytes
     */
    @Override
    public long memorySize() {
        return memoryManager.pool.allocated();
    }

    public int entries() { return size.get(); }

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

    private Rebalancer.RebalanceResult rebalance(Chunk<K, V> c, K key, V value, Computer computer, Operation op) {
        if (c == null) {
            assert op == Operation.NO_OP;
            return null;
        }
        Rebalancer rebalancer = new Rebalancer(c, comparator, true, memoryManager, handleFactory,
                keySerializer, keySizeCalculator, valueSerializer, valueSizeCalculator);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        Rebalancer.RebalanceResult result = rebalancer.createNewChunks(key, value, computer, op); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk<K, V>> newChunks = rebalancer.getNewChunks();
        List<Chunk<K, V>> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        for (Chunk chunk : engaged
                ) {
            chunk.release();
        }

        if (result.success && result.putIfAbsent) {
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

        Chunk<K, V> firstEngaged = engaged.get(0);

        // replace in linked list - we now need to find previous chunk to our chunk
        // and CAS its next to point to c1, which is the same c1 for all threads who reached this point
        // since prev might be marked (in compact itself) - we need to repeat this until successful
        while (true) {
            // start with first chunk (i.e., head)
            Entry<Object, Chunk<K, V>> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

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

    private boolean rebalancePutIfAbsent(Chunk<K, V> c, K key, V value) {
        Rebalancer.RebalanceResult result = rebalance(c, key, value, null, Operation.PUT_IF_ABSENT);
        assert result != null;
        if (!result.success) { // rebalance helped put
            return putIfAbsent(key, value, false);
        }
        memoryManager.stopThread();
        return result.putIfAbsent;
    }

    private void rebalancePut(Chunk<K, V> c, K key, V value) {
        Rebalancer.RebalanceResult result = rebalance(c, key, value, null, Operation.PUT);
        assert result != null;
        if (result.success) { // rebalance helped put
            return;
        }
        put(key, value);
    }

    private void rebalanceCompute(Chunk<K, V> c, K key, V value, Computer computer) {
        Rebalancer.RebalanceResult result = rebalance(c, key, value, computer, Operation.COMPUTE);
        assert result != null;
        if (result.success) { // rebalance helped compute
            return;
        }
        putIfAbsentComputeIfPresent(key, value, computer);
    }

    private boolean rebalanceRemove(Chunk<K, V> c, K key) {
        Rebalancer.RebalanceResult result = rebalance(c, key, null, null, Operation.REMOVE);
        assert result != null;
        return result.success;
    }

    /*-------------- oak.OakMap Methods --------------*/

    @Override
    public void put(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            lookUp.handle.put(value, valueSerializer, valueSizeCalculator, memoryManager);
            memoryManager.stopThread();
            return;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            put(key, value);
            memoryManager.stopThread();
            return;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalancePut(c, key, value);
            memoryManager.stopThread();
            return;
        }

        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                rebalancePut(c, key, value);
                memoryManager.stopThread();
                return;
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                ei = prevEi;
                prevHi = c.getHandleIndex(prevEi);
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            rebalancePut(c, key, value);
            memoryManager.stopThread();
            return;
        }

        c.writeValue(hi, value); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT, ei, hi, prevHi, null);

        // publish put
        if (!c.publish(opData)) {
            rebalancePut(c, key, value);
            memoryManager.stopThread();
            return;
        }

        // set pointer to value
        c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, true);
    }

    public boolean putIfAbsent(K key, V value, boolean startThread) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        if (startThread) {
            memoryManager.startThread();
        }

        Chunk c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            memoryManager.stopThread();
            return false;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            return putIfAbsent(key, value, false);
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            return rebalancePutIfAbsent(c, key, value);
        }


        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                return rebalancePutIfAbsent(c, key, value);
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                Handle handle = c.getHandle(prevEi);
                if (handle == null) {
                    ei = prevEi;
                    prevHi = c.getHandleIndex(prevEi);
                } else {
                    memoryManager.stopThread();
                    return false;
                }
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            return rebalancePutIfAbsent(c, key, value);
        }

        c.writeValue(hi, value); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT_IF_ABSENT, ei, hi, prevHi, null);

        // publish put
        if (!c.publish(opData)) {
            return rebalancePutIfAbsent(c, key, value);
        }

        // set pointer to value
        boolean ret = c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();
        return ret;
    }

    @Override
    public void putIfAbsentComputeIfPresent(K key, V value, Computer computer) {
        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        Chunk c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            if (lookUp.handle.compute(computer, memoryManager)) {
                // compute was successful and handle wasn't found deleted; in case
                // this handle was already found as deleted, continue to construct another handle
                memoryManager.stopThread();
                return;
            }
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            putIfAbsentComputeIfPresent(key, value, computer);
            memoryManager.stopThread();
            return;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalanceCompute(c, key, value, computer);
            memoryManager.stopThread();
            return;
        }

        // we come here when no key was found, which can be in 3 cases:
        // 1. no entry in the linked list at all
        // 2. entry in the linked list, but handle is not attached
        // 3. entry in the linked list, handle attached, but handle is marked deleted
        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                rebalanceCompute(c, key, value, computer);
                memoryManager.stopThread();
                return;
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                Handle handle = c.getHandle(prevEi);
                if (handle == null) {
                    ei = prevEi;
                    prevHi = c.getHandleIndex(prevEi);
                } else {
                    if (handle.compute(computer, memoryManager)) {
                        // compute was successful and handle wasn't found deleted; in case
                        // this handle was already found as deleted, continue to construct another handle
                        memoryManager.stopThread();
                        return;
                    }
                }
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            rebalanceCompute(c, key, value, computer);
            memoryManager.stopThread();
            return;
        }

        c.writeValue(hi, value); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.COMPUTE, ei, hi, prevHi, computer);

        // publish put
        if (!c.publish(opData)) {
            rebalanceCompute(c, key, value, computer);
            memoryManager.stopThread();
            return;
        }

        // set pointer to value
        c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();
    }

    @Override
    public void remove(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        boolean logical = true;
        Handle prev = null;

        memoryManager.startThread();

        while (true) {

            Chunk c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp != null && logical) {
                prev = lookUp.handle; // remember previous handle
            }
            if (!logical && lookUp != null && prev != lookUp.handle) {
                memoryManager.stopThread();
                return;  // someone else used this entry
            }

            if (lookUp == null || lookUp.handle == null) {
                memoryManager.stopThread();
                return;
            }

            if (logical) {
                if (!lookUp.handle.remove(memoryManager)) {
                    memoryManager.stopThread();
                    return;
                }
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            Chunk.State state = c.state();
            if (state == Chunk.State.INFANT) {
                // the infant is already connected so rebalancer won't add this put
                rebalance(c.creator(), null, null, null, Operation.NO_OP);
                logical = false;
                continue;
            }
            if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
                if (!rebalanceRemove(c, key)) {
                    logical = false;
                    continue;
                }
                memoryManager.stopThread();
                return;
            }


            assert lookUp.entryIndex > 0;
            assert lookUp.handleIndex > 0;
            Chunk.OpData opData = new Chunk.OpData(Operation.REMOVE, lookUp.entryIndex, -1, lookUp.handleIndex, null);

            // publish
            if (!c.publish(opData)) {
                if (!rebalanceRemove(c, key)) {
                    logical = false;
                    continue;
                }
                memoryManager.stopThread();
                return;
            }

            // set pointer to value
            c.pointToValue(opData);

            c.unpublish(opData);

            checkRebalance(c);

            memoryManager.stopThread();

            return;

        }
    }

    @Override
    public V get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();
        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) {
            return null;
        }
        lookUp.handle.readLock.lock();
        V value;
        try {
            ByteBuffer serializedValue = lookUp.handle.getImmutableByteBuffer();
            value = valueDeserializer.deserialize(serializedValue);
        } finally {
            lookUp.handle.readLock.unlock();
            memoryManager.stopThread();
        }
        return value;
    }

    @Override
    public <T> T getTransformation(K key, Function<ByteBuffer,T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();
        Chunk c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) {
            return null;
        }

        T transformation;
        lookUp.handle.readLock.lock();
        try {
            ByteBuffer value = lookUp.handle.getImmutableByteBuffer();
            transformation = transformer.apply(value);
        } finally {
            lookUp.handle.readLock.unlock();
            memoryManager.stopThread();
        }
        return transformation;
    }

    @Override
    public K getMinKey() {
        memoryManager.startThread();
        Chunk c = skiplist.firstEntry().getValue();
        ByteBuffer serializedMinKey = c.readMinKey();
        K minKey = keyDeserializer.deserialize(serializedMinKey);
        memoryManager.stopThread();
        return minKey;
    }

    @Override
    public K getMaxKey() {
        memoryManager.startThread();
        Chunk<K, V> c = skiplist.lastEntry().getValue();
        Chunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }

        ByteBuffer serializedMaxKey = c.readMaxKey();
        K maxKey = keyDeserializer.deserialize(serializedMaxKey);
        memoryManager.stopThread();
        return maxKey;
    }

    @Override
    public boolean computeIfPresent(K key, Computer computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();
        Chunk c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) return false;

        memoryManager.stopThread();

        lookUp.handle.compute(computer, memoryManager);
        return true;
    }

    @Override
    public OakMap subMap(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive) {
        if (fromKey == null || toKey == null)
            throw new NullPointerException();
        return new SubOakMap(this, fromKey, fromInclusive, toKey, toInclusive, false);
    }

    @Override
    public OakMap headMap(Object toKey, boolean inclusive) {
        if (toKey == null)
            throw new NullPointerException();
        return new SubOakMap(this, null, false, toKey, inclusive, false);
    }

    @Override
    public OakMap tailMap(Object fromKey, boolean inclusive) {
        if (fromKey == null)
            throw new NullPointerException();
        return new SubOakMap(this, fromKey, inclusive, null, false, false);
    }

    @Override
    public OakMap descendingMap() {
        OakMap dm = descendingMap;
        return (dm != null) ? dm : (descendingMap = new SubOakMap
                (this, null, false, null, false, true));
    }

    // encapsulates finding of the chunk in the skip list and later chunk list traversal
    private Chunk findChunk(Object key) {
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);
        return c;
    }

    /*-------------- Iterators --------------*/

    /**
     * Base of iterator classes:
     */
    abstract class Iter<T> implements CloseableIterator<T> {

        /**
         * the next node to return from next();
         */
        Chunk<K, V> nextChunk;
        Chunk.AscendingIter nextChunkIter;
        int next;
        /**
         * Cache of next value field to maintain weak consistency
         */
        Handle nextHandle;

        /**
         * Initializes ascending iterator for entire range.
         */
        Iter() {
            memoryManager.startThread();
            next = Chunk.NONE;
            nextHandle = null;
            initChunk();
            findNextKey();
        }

        @Override
        public void close() {
            memoryManager.stopThread();
        }

        public final boolean hasNext() {
            return next != Chunk.NONE;
        }

        /**
         * Advances next to higher entry.
         */
        void advance() {
            if (next == Chunk.NONE)
                throw new NoSuchElementException();
            findNextKey();
        }

        /**
         * used only to init the iterator
         */
        private void initChunk() {
//            ByteBuffer b = constructMinKeyBuffer();
            nextChunk = skiplist.floorEntry(minKey).getValue();
            if (nextChunk == null) {
                nextChunkIter = null;
            } else {
                nextChunkIter = nextChunk.ascendingIter();
            }
        }

        /**
         * sets nextChunk, nextChunkIter, next and nextValue to the next key
         * if such a key doesn't exists then next is set to oak.Chunk.NONE and nextValue is set to null
         */
        private void findNextKey() {
            if (nextChunkIter == null) {
                return;
            }
            // find first chunk that has a valid key (chunks can have only removed keys)
            while (!nextChunkIter.hasNext()) {
                nextChunk = nextChunk.next.getReference(); // try next chunk
                if (nextChunk == null) { // there is no next chunk
                    next = Chunk.NONE;
                    nextHandle = null;
                    return;
                }
                nextChunkIter = nextChunk.ascendingIter();
            }

            next = nextChunkIter.next();

            // set next value
            Handle h = nextChunk.getHandle(next);
            if (h != null) {
                nextHandle = h;
            } else {
                nextHandle = null;
            }

        }

        ByteBuffer getKey(int ki, Chunk c) {
            return c.readKey(ki);
        }

    }

    class ValueIterator extends Iter<V> {

        public V next() {
            Handle handle = nextHandle;
            advance();
            if (handle == null)
                return null;
            handle.readLock.lock();
            V value = valueDeserializer.deserialize(handle.getImmutableByteBuffer());
            handle.readLock.unlock();
            return value;
        }
    }

    class EntryIterator extends Iter<Map.Entry<K, V>> {

        public Map.Entry<K, V> next() {
            int n = next;
            Chunk c = nextChunk;
            Handle handle = nextHandle;
            advance();
            if (handle == null)
                return null;
            handle.readLock.lock();
            V value = valueDeserializer.deserialize(handle.getImmutableByteBuffer());
            handle.readLock.unlock();
            ByteBuffer serializedKey = getKey(n, c);
            serializedKey = serializedKey.slice(); // TODO can I get rid of this?
            K key = keyDeserializer.deserialize(serializedKey);
            return new AbstractMap.SimpleImmutableEntry<K, V>(key, value);
        }
    }

    class KeyIterator extends Iter<K> {

        public K next() {
            int n = next;
            Chunk c = nextChunk;
            advance();
            ByteBuffer serializedKey = getKey(n, c);
            serializedKey = serializedKey.slice(); // TODO can I get rid of this?
            K key = keyDeserializer.deserialize(serializedKey);
            return key;
        }
    }

    class ValueTransformIterator<T> extends Iter<T> {

        Function<ByteBuffer, T> transformer;

        public ValueTransformIterator(Function<ByteBuffer, T> transformer) {
            super();
            this.transformer = transformer;
        }

        public T next() {
            T transformation = null;
            Handle handle = nextChunk.getHandle(next);
            handle.readLock.lock();
            try {
                ByteBuffer value = handle.getImmutableByteBuffer();
                transformation = transformer.apply(value);
            } finally {
                handle.readLock.unlock();
            }
            advance();
            return transformation;
        }
    }

    class EntryTransformIterator<T> extends Iter<T> {

        Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

        public EntryTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            super();
            this.transformer = transformer;
        }

        public T next() {
            T transformation = null;
            Handle handle = nextChunk.getHandle(next);
            ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
            handle.readLock.lock();
            try {
                ByteBuffer value = handle.getImmutableByteBuffer();
                Map.Entry<ByteBuffer, ByteBuffer> entry = new AbstractMap.SimpleImmutableEntry<>(key, value);
                transformation = transformer.apply(entry);
            } finally {
                handle.readLock.unlock();
            }
            advance();
            return transformation;
        }
    }

    class KeyTransformIterator<T> extends Iter<T> {

        Function<ByteBuffer, T> transformer;

        public KeyTransformIterator(Function<ByteBuffer, T> transformer) {
            super();
            this.transformer = transformer;
        }

        public T next() {
            ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
            return transformer.apply(key);
        }
    }

// Factory methods for iterators

    @Override
    public CloseableIterator<V> valuesIterator() {
        return new ValueIterator();
    }

    @Override
    public CloseableIterator<Map.Entry<K, V>> entriesIterator() {
        return new EntryIterator();
    }

    @Override
    public CloseableIterator<K> keysIterator() {
        return new KeyIterator();
    }

    @Override
    public <T> CloseableIterator<T> valuesTransformIterator(Function<ByteBuffer,T> transformer) {
        return new ValueTransformIterator<T>(transformer);
    }

    @Override
    public <T> CloseableIterator<T> entriesTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
        return new EntryTransformIterator<T>(transformer);
    }

    @Override
    public <T> CloseableIterator<T> keysTransformIterator(Function<ByteBuffer,T> transformer) {
        return new KeyTransformIterator<T>(transformer);
    }

    /* ---------------- Sub Map -------------- */

    public class SubOakMap<K, V> implements OakMap<K, V> {
        /**
         * underlying map
         */
        private final OakMapOffHeapImpl oak;
        /**
         * lower bound key, or null if from start
         */
        private final K lo;
        /**
         * upper bound key, or null if to end
         */
        private final K hi;
        /**
         * inclusion flag for lo
         */
        private final boolean loInclusive;
        /**
         * inclusion flag for hi
         */
        private final boolean hiInclusive;
        /**
         * direction
         */
        private final boolean isDescending;

        /**
         * Creates a new submap, initializing all fields.
         */
        public SubOakMap(OakMapOffHeapImpl oak,
                         K fromKey, boolean fromInclusive,
                         K toKey, boolean toInclusive,
                  boolean isDescending) {
            if (fromKey != null && toKey != null &&
                    oak.comparator.compare(fromKey, toKey) > 0)
                throw new IllegalArgumentException("inconsistent range");
            this.oak = oak;
            this.lo = fromKey;
            this.hi = toKey;
            this.loInclusive = fromInclusive;
            this.hiInclusive = toInclusive;
            this.isDescending = isDescending;
        }

        /**
         * @return current off heap memory usage of the original oak in bytes
         */
        @Override
        public long memorySize() {
            return oak.memorySize();
        }

        /*-------------- Utilities --------------*/

        int keyCompare(Object k1, Object k2) {
            return oak.comparator.compare(k1, k2);
        }

        boolean tooLow(Object key) {
            int c;
            return (lo != null && ((c = keyCompare(key, lo)) < 0 ||
                    (c == 0 && !loInclusive)));
        }

        boolean tooHigh(Object key) {
            int c;
            return (hi != null && ((c = keyCompare(key, hi)) > 0 ||
                    (c == 0 && !hiInclusive)));
        }

        boolean inBounds(Object key) {
            return !tooLow(key) && !tooHigh(key);
        }


        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        OakMapOffHeapImpl.SubOakMap newSubOakMap(K fromKey, boolean fromInclusive,
                                                 K toKey, boolean toInclusive) {
            if (isDescending) { // flip senses
                // TODO look into this
                K tk = fromKey;
                fromKey = toKey;
                toKey = tk;
                boolean ti = fromInclusive;
                fromInclusive = toInclusive;
                toInclusive = ti;
            }
            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                } else {
                    int c = keyCompare(fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                } else {
                    int c = keyCompare(toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new OakMapOffHeapImpl.SubOakMap(oak, fromKey, fromInclusive,
                    toKey, toInclusive, isDescending);
        }

        /*-------------- oak.OakMap Methods --------------*/

        @Override
        public void put(K key, V value) {
            oak.put(key, value);
        }

        @Override
        public boolean putIfAbsent(K key, V value) {
            return oak.putIfAbsent(key, value);
        }

        @Override
        public void putIfAbsentComputeIfPresent(K key, V value, Computer computer) {
            oak.putIfAbsentComputeIfPresent(key, value, computer);
        }

        @Override
        public void remove(K key) {
            if (inBounds(key)) {
                oak.remove(key);
            }
        }

        @Override
        public V get(K key) {
            if (key == null) throw new NullPointerException();
            return (!inBounds(key)) ? null : (V) oak.get(key);
        }

        @Override
        public <T> T getTransformation(K key, Function<ByteBuffer,T> transformer) {
            if (key == null || transformer == null) throw new NullPointerException();
            return (!inBounds(key)) ? null : (T) oak.getTransformation(key, transformer);
        }

        @Override
        public boolean computeIfPresent(K key, Computer computer) {
            return oak.computeIfPresent(key, computer);
        }

        @Override
        public OakMap subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubOakMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
        public OakMap headMap(K toKey, boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubOakMap(null, false, toKey, inclusive);
        }

        @Override
        public OakMap tailMap(K fromKey, boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubOakMap(fromKey, inclusive, null, false);
        }

        @Override
        public OakMap descendingMap() {
            return new SubOakMap(oak, lo, loInclusive,
                    hi, hiInclusive, !isDescending);
        }

        @Override
        public K getMinKey() {
            return (K) oak.getMinKey();
        }

        @Override
        public K getMaxKey() {
            return (K) oak.getMaxKey();
        }

        /*-------------- Iterators --------------*/

        abstract class SubOakMapIter<T> implements CloseableIterator<T> {

            /**
             * the next node to return from next();
             */
            Chunk<K, V> nextChunk;
            Chunk.ChunkIter nextChunkIter;
            int next;
            /**
             * Cache of next value field to maintain weak consistency
             */
            Handle nextHandle;

            SubOakMapIter() {
                memoryManager.startThread();
                next = Chunk.NONE;
                nextHandle = null;
                initChunk();
                findNextKeyInRange();
            }

            @Override
            public void close() {
                memoryManager.stopThread();
            }

            public final boolean hasNext() {
                return next != Chunk.NONE;
            }

            /**
             * Advances next to higher entry.
             */
            void advance() {
                if (next == Chunk.NONE)
                    throw new NoSuchElementException();
                findNextKeyInRange();
            }

            /**
             * used only to init the iterator
             */
            private void initChunk() {
                if (!isDescending) {
                    if (lo != null)
                        nextChunk = (Chunk<K, V>) oak.skiplist.floorEntry(lo).getValue();
                    else
                        nextChunk = (Chunk<K, V>) oak.skiplist.floorEntry(oak.minKey).getValue();
                    if (nextChunk == null) {
                        nextChunkIter = null;
                    } else {
                        nextChunkIter = lo != null ? nextChunk.ascendingIter(lo) : nextChunk.ascendingIter();
                    }
                } else {
                    nextChunk = hi != null ? (Chunk<K, V>) oak.skiplist.floorEntry(hi).getValue()
                            : (Chunk<K, V>) oak.skiplist.lastEntry().getValue();
                    if (nextChunk == null) {
                        nextChunkIter = null;
                    } else {
                        nextChunkIter = hi != null ? nextChunk.descendingIter(hi, hiInclusive) : nextChunk.descendingIter();
                    }
                }
            }

            /**
             * sets nextChunk, nextChunkIter, next and nextValue to the next key in range
             * if such a key doesn't exists then next is set to oak.Chunk.NONE and nextValue is set to null
             */
            private void findNextKeyInRange() {
                if (nextChunkIter == null) {
                    return;
                }
                if (!isDescending) {
                    ascend();
                } else {
                    descend();
                }
            }

            private boolean findNextChunk() {
                while (!nextChunkIter.hasNext()) { // chunks can have only removed keys
                    nextChunk = nextChunk.next.getReference(); // try next chunk
                    if (nextChunk == null) { // there is no next chunk
                        next = Chunk.NONE;
                        nextHandle = null;
                        return false;
                    }
                    nextChunkIter = nextChunk.ascendingIter();
                }
                return true;
            }

            private void ascend() {
                if (!findNextChunk()) {
                    return;
                }
                // now look for first key in range
                next = nextChunkIter.next();
                ByteBuffer key = getKey(next, nextChunk);

                while (!inBounds(key)) {
                    if (tooHigh(key)) {
                        // if we reached a key that is too high then there is no key in range
                        next = Chunk.NONE;
                        nextHandle = null;
                        return;
                    }
                    if (!findNextChunk()) {
                        return;
                    }
                    next = nextChunkIter.next();
                    key = getKey(next, nextChunk);
                }
                // set next value
                Handle h = nextChunk.getHandle(next);
                if (h != null) {
                    nextHandle = h;
                } else {
                    nextHandle = null;
                }
            }

            private boolean findPrevChunk() {
                while (!nextChunkIter.hasNext()) {
                    ByteBuffer minKey = nextChunk.minKey;
                    Map.Entry e = oak.skiplist.lowerEntry(minKey);
                    if (e == null) { // there no next chunk
                        assert minKey == oak.minKey;
                        next = Chunk.NONE;
                        nextHandle = null;
                        return false;
                    }
                    nextChunk = (Chunk) e.getValue();
                    Chunk nextNext = nextChunk.next.getReference();
                    if (nextNext == null) {
                        nextChunkIter = nextChunk.descendingIter((K) keyDeserializer.deserialize(minKey), false);
                        continue;
                    }
                    ByteBuffer nextMinKey = nextNext.minKey;
                    if (comparator.compare(nextMinKey, minKey) < 0) {
                        nextChunk = nextNext;
                        nextMinKey = nextChunk.next.getReference().minKey;
                        while (comparator.compare(nextMinKey, minKey) < 0) {
                            nextChunk = nextChunk.next.getReference();
                            nextMinKey = nextChunk.next.getReference().minKey;
                        }
                    }
                    nextChunkIter = nextChunk.descendingIter((K) keyDeserializer.deserialize(minKey), false); // TODO check correctness
                }
                return true;
            }

            private void descend() {
                if (!findPrevChunk()) {
                    return;
                }
                // now look for first key in range
                next = nextChunkIter.next();
                ByteBuffer key = getKey(next, nextChunk);

                while (!inBounds(key)) {
                    if (tooLow(key)) {
                        // if we reached a key that is too low then there is no key in range
                        next = Chunk.NONE;
                        nextHandle = null;
                        return;
                    }
                    if (!findPrevChunk()) {
                        return;
                    }
                    next = nextChunkIter.next();
                    key = getKey(next, nextChunk);
                }
                nextHandle = nextChunk.getHandle(next); // set next value
            }

            ByteBuffer getKey(int ki, Chunk c) {
                return c.readKey(ki);
            }

        }

        class SubOakValueIterator extends SubOakMapIter<V> {

            SubOakValueIterator() {
                super();
            }

            public V next() {
                Handle handle = nextHandle;
                advance();
                if (handle == null)
                    return null;
                handle.readLock.lock();
                V value = (V) oak.valueDeserializer.deserialize(handle.getImmutableByteBuffer());
                handle.readLock.unlock();
                return value;
            }
        }

        class SubOakEntryIterator extends SubOakMapIter<Map.Entry<K, V>> {

            SubOakEntryIterator() {
                super();
            }

            public Map.Entry<K, V> next() {
                int n = next;
                Chunk c = nextChunk;
                Handle handle = nextHandle;
                if (handle == null)
                    return null;
                advance();
                handle.readLock.lock();
                V value = (V) oak.valueDeserializer.deserialize(handle.getImmutableByteBuffer());
                handle.readLock.unlock();
                ByteBuffer serializedKey = getKey(n, c);
                serializedKey = serializedKey.slice(); // TODO can I get rid of this?
                K key = (K) keyDeserializer.deserialize(serializedKey);
                return new AbstractMap.SimpleImmutableEntry<K, V>(key, value);
            }
        }

        class SubOakKeyIterator extends SubOakMapIter<K> {

            public K next() {
                int n = next;
                Chunk c = nextChunk;
                advance();
                ByteBuffer serializedKey = getKey(n, c);
                serializedKey = serializedKey.slice(); // TODO can I get rid of this?
                K key = (K) keyDeserializer.deserialize(serializedKey);
                return key;
            }
        }

        class SubOakValueTransformIterator<T> extends SubOakMapIter<T> {

            Function<ByteBuffer, T> transformer;

            public SubOakValueTransformIterator(Function<ByteBuffer, T> transformer) {
                super();
                this.transformer = transformer;
            }

            public T next() {
                T transformation = null;
                Handle handle = nextChunk.getHandle(next);
                ByteBuffer value = handle.getImmutableByteBuffer();
                advance();
                handle.readLock.lock();
                try {
                    transformation = transformer.apply(value);
                } finally {
                    handle.readLock.unlock();
                }
                return transformation;
            }
        }

        class SubOakEntryTransformIterator<T> extends SubOakMapIter<T> {

            Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

            public SubOakEntryTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
                super();
                this.transformer = transformer;
            }

            public T next() {
                T transformation = null;
                Handle handle = nextChunk.getHandle(next);
                ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
                ByteBuffer value = handle.getImmutableByteBuffer();
                Map.Entry<ByteBuffer, ByteBuffer> entry = new AbstractMap.SimpleImmutableEntry<>(key, value);
                advance();
                handle.readLock.lock();
                try {
                    transformation = transformer.apply(entry);
                } finally {
                    handle.readLock.unlock();
                }
                return transformation;
            }
        }

        class SubOakKeyTransformIterator<T> extends SubOakMapIter<T> {

            Function<ByteBuffer, T> transformer;

            public SubOakKeyTransformIterator(Function<ByteBuffer, T> transformer) {
                super();
                this.transformer = transformer;
            }

            public T next() {
                ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
                return transformer.apply(key);
            }
        }

        // Factory methods for iterators

        @Override
        public CloseableIterator<V> valuesIterator() {
            return new SubOakValueIterator();
        }

        @Override
        public CloseableIterator<Map.Entry<K, V>> entriesIterator() {
            return new SubOakEntryIterator();
        }

        @Override
        public CloseableIterator<K> keysIterator() {
            return new SubOakKeyIterator();
        }

        @Override
        public <T> CloseableIterator<T> valuesTransformIterator(Function<ByteBuffer,T> transformer) {
            return new SubOakValueTransformIterator<T>(transformer);
        }

        @Override
        public <T> CloseableIterator<T> entriesTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            return new SubOakEntryTransformIterator<T>(transformer);
        }

        @Override
        public <T> CloseableIterator<T> keysTransformIterator(Function<ByteBuffer,T> transformer) {
            return new SubOakKeyTransformIterator<T>(transformer);
        }

    }

}
