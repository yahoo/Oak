/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;


import java.io.IOException;
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

import static com.oath.oak.Chunk.*;
import static com.oath.oak.ValueUtils.INVALID_VERSION;
import static com.oath.oak.ValueUtils.ValueResult.*;
import static com.oath.oak.UnsafeUtils.longToInts;

class InternalOakMap<K, V> {

    /*-------------- Members --------------*/

    final ConcurrentSkipListMap<Object, Chunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
    private int[] entriesHash = null;    // immutable hash of entries for a super-fast navigation
    private final AtomicReference<Chunk<K, V>> head;
    private final ByteBuffer minKey;
    private final OakComparator<K> comparator;
    private final MemoryManager memoryManager;
    private final AtomicInteger size;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    // The reference count is used to count the upper objects wrapping this internal map:
    // OakMaps (including subMaps and Views) when all of the above are closed,
    // his map can be closed and memory released.
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private final ValueUtils valueOperator;
    private final static int KEY_HEADER_SIZE = 0;
    /*-------------- Constructors --------------*/

    /**
     * init with capacity = 2g
     */

    InternalOakMap(K minKey, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
                   OakComparator<K> oakComparator, MemoryManager memoryManager, int chunkMaxItems,
                   ValueUtils valueOperator) {

        this.size = new AtomicInteger(0);
        this.memoryManager = memoryManager;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.comparator = oakComparator;

        this.minKey = ByteBuffer.allocate(this.keySerializer.calculateSize(minKey));
        this.minKey.position(0);
        this.keySerializer.serialize(minKey, this.minKey);

        // This is a trick for letting us search through the skiplist using both serialized and unserialized keys.
        // Might be nicer to replace it with a proper visitor
        Comparator<Object> mixedKeyComparator = (o1, o2) -> {
            if (o1 instanceof ByteBuffer) {
                if (o2 instanceof ByteBuffer) {
                    return oakComparator.compareSerializedKeys((ByteBuffer) o1, (ByteBuffer) o2);
                } else {
                    // Note the inversion of arguments, hence sign flip
                    return (-1) * oakComparator.compareKeyAndSerializedKey((K) o2, (ByteBuffer) o1);
                }
            } else {
                if (o2 instanceof ByteBuffer) {
                    return oakComparator.compareKeyAndSerializedKey((K) o1, (ByteBuffer) o2);
                } else {
                    return oakComparator.compareKeys((K) o1, (K) o2);
                }
            }
        };
        this.skiplist = new ConcurrentSkipListMap<>(mixedKeyComparator);

        Chunk<K, V> head = new Chunk<>(this.minKey, null, this.comparator, memoryManager, chunkMaxItems,
                this.size, keySerializer, valueSerializer, valueOperator);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);
        this.valueOperator = valueOperator;
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
            try {
                memoryManager.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
    private Chunk<K, V> iterateChunks(Chunk<K, V> c, K key) {
        // find chunk following given chunk (next)
        Chunk<K, V> next = c.next.getReference();

        // since skiplist isn't updated atomically in split/compaction, our key might belong in the next chunk
        // we need to iterate the chunks until we find the correct one
        while ((next != null) && (comparator.compareKeyAndSerializedKey(key, next.minKey) >= 0)) {
            c = next;
            next = c.next.getReference();
        }

        return c;
    }


    /**
     * @param c - Chunk to rebalance
     */
    private void rebalance(Chunk<K, V> c) {

        if (c == null) {
            return;
        }
        Rebalancer<K, V> rebalancer = new Rebalancer<>(c, true, memoryManager, keySerializer,
                valueSerializer, valueOperator);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        rebalancer.createNewChunks(); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk<K, V>> newChunks = rebalancer.getNewChunks();
        List<Chunk<K, V>> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        engaged.forEach(Chunk::release);
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
            if (curr == null) {
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

    private boolean inTheMiddleOfRebalance(Chunk<K, V> c) {
        State state = c.state();
        if (state == State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator());
            return true;
        }
        if (state == State.FROZEN || state == State.RELEASED) {
            rebalance(c);
            return true;
        }
        return false;
    }

    private boolean finalizeDeletion(Chunk<K, V> c, LookUp lookUp) {
        if (lookUp != null) {
            if (c.finalizeDeletion(lookUp)) {
                rebalance(c);
                return true;
            }
        }
        return false;
    }

    /**
     * This function completes the insertion of the value reference in the variable {@code lookUp}, and updates the
     * value's version in {@code lookUp}. In case, the linking cannot be done (i.e., a concurrent rebalance), than
     * rebalance is called.
     *
     * @param c      - the chuck pointed by {@code lookUp}
     * @param lookUp - holds the value reference, old version, and relevant entry to update
     * @return whether the caller method should restart (if a rebalance was executed).
     */
    private boolean updateVersionAfterLinking(Chunk<K, V> c, LookUp lookUp) {
        if (c.completeLinking(lookUp) == INVALID_VERSION) {
            rebalance(c);
            return true;
        }
        return false;
    }

    /*-------------- OakMap Methods --------------*/

    V put(K key, V value, Function<ByteBuffer, V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in put");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            // If there is a matching value reference for the given key, and it is not marked as deleted, then this put
            // changes the slice pointed by this value reference.
            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking(c, lookUp)) {
                    continue;
                }
                Result<V> res = valueOperator.exchange(c, lookUp, value, transformer, valueSerializer, memoryManager);
                if (res.operationResult == TRUE) {
                    return res.value;
                }
                // Exchange failed because the value was deleted/moved between lookup and exchange. Continue with
                // insertion.
                continue;
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            /* If {@code lookUp == null} i.e., there is no entry with that key in Oak, the previous version
            associated with the entry is INVALID_VERSION.
            Otherwise, there is an entry with the given key and its value is deleted (or at least in the process of
            being deleted). After calling {@code finalizeDeletion(Chunk<K, V>, LookUp)}, the version of this entry
            should be negative.
            Concurrent insertions, however, may cause the version to become valid again.
            Thus, to ensure that the operation behaves as if it itself unlinked the deleted value, we take the minus
            the absolute value of the version written in lookUp (this way, if the version is already negative, it
            remains negative).
             */
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);

            int ei;
            if (lookUp != null) {
                ei = lookUp.entryIndex;
                assert ei > 0;
            } else {
                ei = c.allocateEntryAndKey(key);
                if (ei == INVALID_ENTRY_INDEX) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ei, key);
                if (prevEi != ei) {
                    c.releaseKey(ei);
                    if (c.getValueReference(prevEi) != INVALID_VALUE_REFERENCE) {
                        continue;
                    }
                    // We use an existing entry only if its value reference is INVALID_VALUE_REFERENCE
                    ei = prevEi;
                }
            }

            int[] version = new int[1];
            long newValueReference = c.writeValue(value, version); // write value in place

            Chunk.OpData opData = new Chunk.OpData(ei, INVALID_VALUE_REFERENCE, newValueReference, oldVersion,
                    version[0]);

            if (!c.publish()) {
                c.releaseValue(newValueReference);
                rebalance(c);
                continue;
            }

            if (c.linkValue(opData) != TRUE) {
                c.releaseValue(newValueReference);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return null;
            }
        }
    }

    Result<V> putIfAbsent(K key, V value, Function<ByteBuffer, V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in putIfAbsent");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking(c, lookUp)) {
                    continue;
                }
                if (transformer == null) {
                    return Result.withFlag(FALSE);
                }
                Result<V> res = valueOperator.transform(lookUp.valueSlice, transformer, lookUp.version);
                if (res.operationResult == TRUE) {
                    return res;
                }
                continue;
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            /* If {@code lookUp == null} i.e., there is no entry with that key in Oak, the previous version
            associated with the entry is INVALID_VERSION.
            Otherwise, there is an entry with the given key and its value is deleted (or at least in the process of
            being deleted). After calling {@code finalizeDeletion(Chunk<K, V>, LookUp)}, the version of this entry
            should be negative.
            Concurrent insertions, however, may cause the version to become valid again.
            Thus, to ensure that the operation behaves as if it itself unlinked the deleted value, we take the minus
            the absolute value of the version written in lookUp (this way, if the version is already negative, it
            remains negative).
             */
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);

            int ei;
            if (lookUp != null) {
                // There's an entry for this key, but it isn't linked to any value (in which case valueReference is
                // DELETED_VALUE)
                // or it's linked to a deleted value that is referenced by valueReference (a valid one)
                ei = lookUp.entryIndex;
                assert ei > 0;
            } else {
                ei = c.allocateEntryAndKey(key);
                if (ei == INVALID_ENTRY_INDEX) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ei, key);
                if (prevEi != ei) {
                    c.releaseKey(ei);
                    // some other thread linked its entry with the same key.
                    int[] otherVersion = new int[1];
                    Slice otherSlice = c.buildValueSlice(c.getValueReferenceAndVersion(prevEi, otherVersion));
                    if (otherSlice != null) {
                        if (transformer == null) {
                            return Result.withFlag(FALSE);
                        }
                        Result<V> res = valueOperator.transform(otherSlice, transformer, otherVersion[0]);
                        if (res.operationResult == TRUE) {
                            return res;
                        }
                        continue;
                    } else {
                        // both threads compete for the put
                        ei = prevEi;
                    }
                }
            }

            int[] version = new int[1];
            long newValueReference = c.writeValue(value, version); // write value in place

            Chunk.OpData opData = new Chunk.OpData(ei, INVALID_VALUE_REFERENCE, newValueReference, oldVersion,
                    version[0]);

            if (!c.publish()) {
                c.releaseValue(newValueReference);
                rebalance(c);
                continue;
            }

            if (c.linkValue(opData) != TRUE) {
                c.releaseValue(newValueReference);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return transformer == null ? Result.withFlag(TRUE) : Result.withValue(null);
            }
        }
    }

    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer) {
        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in putIfAbsentComputeIfPresent");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking(c, lookUp)) {
                    continue;
                }
                ValueUtils.ValueResult res = valueOperator.compute(lookUp.valueSlice, computer, lookUp.version);
                if (res == TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already found as deleted, continue to allocate a new value slice
                    return false;
                } else if (res == RETRY) {
                    continue;
                }
            }

            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            /* If {@code lookUp == null} i.e., there is no entry with that key in Oak, the previous version
            associated with the entry is INVALID_VERSION.
            Otherwise, there is an entry with the given key and its value is deleted (or at least in the process of
            being deleted). After calling {@code finalizeDeletion(Chunk<K, V>, LookUp)}, the version of this entry
            should be negative.
            Concurrent insertions, however, may cause the version to become valid again.
            Thus, to ensure that the operation behaves as if it itself unlinked the deleted value, we take the minus
            the absolute value of the version written in lookUp (this way, if the version is already negative, it
            remains negative).
             */
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);

            // we come here when no key was found, which can be in 3 cases:
            // 1. no entry in the linked list at all
            // 2. entry in the linked list, but the value reference is INVALID_VALUE_REFERENCE
            // 3. entry in the linked list, the value referenced is marked as deleted
            int ei;
            if (lookUp != null) {
                ei = lookUp.entryIndex;
                assert ei > 0;
            } else {
                ei = c.allocateEntryAndKey(key);
                if (ei == INVALID_ENTRY_INDEX) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ei, key);
                if (prevEi != ei) {
                    c.releaseKey(ei);
                    if (c.getValueReference(prevEi) != INVALID_VALUE_REFERENCE) {
                        continue;
                    } else {
                        ei = prevEi;
                    }
                }
            }

            int[] version = new int[1];
            long newValueReference = c.writeValue(value, version); // write value in place

            Chunk.OpData opData = new Chunk.OpData(ei, INVALID_VALUE_REFERENCE, newValueReference, oldVersion,
                    version[0]);

            if (!c.publish()) {
                c.releaseValue(newValueReference);
                rebalance(c);
                continue;
            }

            if (c.linkValue(opData) != TRUE) {
                c.releaseValue(newValueReference);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return true;
            }
        }
    }

    Result<V> remove(K key, V oldValue, Function<ByteBuffer, V> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        // when logicallyDeleted is true, it means we have marked the value as deleted.
        // Note that the entry will remain linked until rebalance happens.
        boolean logicallyDeleted = false;
        V v = null;

        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in remove");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp == null) {
                // There is no such key. If we did logical deletion and someone else did the physical deletion,
                // then the old value is saved in v. Otherwise v is (correctly) null
                return transformer == null ? Result.withFlag(logicallyDeleted ? TRUE : FALSE) : Result.withValue(v);
            } else if (lookUp.valueSlice == null) {
                if (!c.finalizeDeletion(lookUp)) {
                    return transformer == null ? Result.withFlag(logicallyDeleted ? TRUE : FALSE) : Result.withValue(v);
                }
                rebalance(c);
                continue;
            }

            if (inTheMiddleOfRebalance(c) || updateVersionAfterLinking(c, lookUp)) {
                continue;
            }

            if (logicallyDeleted) {
                // This is the case where we logically deleted this entry (marked the value as deleted), but someone
                // reused the entry before we unlinked it. We have the previous value saved in v.
                return transformer == null ? Result.withFlag(TRUE) : Result.withValue(v);
            } else {
                Result<V> removeResult = valueOperator.remove(lookUp.valueSlice, memoryManager, lookUp.version,
                        oldValue, transformer);
                if (removeResult.operationResult == FALSE) {
                    // we didn't succeed to remove the value: it didn't contain oldValue, or was already marked
                    // as deleted by someone else)
                    return Result.withFlag(FALSE);
                } else if (removeResult.operationResult == RETRY) {
                    continue;
                }
                // we have marked this value as deleted (successful remove)
                logicallyDeleted = true;
                v = removeResult.value;
            }

            assert lookUp.entryIndex > 0;
            assert lookUp.valueReference != INVALID_VALUE_REFERENCE;

            // publish
            if (c.finalizeDeletion(lookUp)) {
                rebalance(c);
            }
        }
    }

    OakRBuffer get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
        while (entriesHash != null) {   // using while in order to be able to break from the block
            // we have hash that can expedite the search
            int hashIndex = keySerializer.hash(key);
            if (hashIndex < 0) {  // user didn't implemented a hash, or implemented it wrongly
              break;
            }
            // each integer index need to be transformed to the index in the "entries array",
            // meaning need to skipp all integers of one entry and add 1 for the header integer
            //hashIndex = (hashIndex*ENTRIES_FIELDS+1)%(entriesHash.length-ENTRIES_FIELDS);

            // 1. limit hash within the addresses possible in the entriesHash
            hashIndex = hashIndex%((entriesHash.length-1)/ENTRIES_FIELDS);
            // 2. Get to correct address: skip in ENTRIES_FIELDS steps and add for header
            hashIndex = hashIndex*ENTRIES_FIELDS+1;

            long keyReference = Chunk.getKeyReference(hashIndex, entriesHash); // find key
            ByteBuffer keyBB = getKeyByteBuffer(keyReference);
            // compare key
            int cmp = comparator.compareKeyAndSerializedKey(key, keyBB);
            if (cmp != 0) {
                // due to collision this is wrong key, hash can not help us, continue through index
                break;
            }
            // keys match, return its value
            int[] valueVersion = new int[1];
            long valueReference =
                Chunk.getValueReferenceAndVersion(hashIndex, valueVersion, entriesHash);
            return new OakRValueBufferImpl(valueReference, valueVersion[0], keyReference, valueOperator,
                memoryManager, this);
        }

        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in get");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp == null || lookUp.valueSlice == null) {
                return null;
            }
            if (updateVersionAfterLinking(c, lookUp)) {
                continue;
            }
            long keyReference = c.getKeyReference(lookUp.entryIndex);
            return new OakRValueBufferImpl(lookUp.valueReference, lookUp.version, keyReference, valueOperator,
                    memoryManager, this);
        }
    }

    boolean computeIfPresent(K key, Consumer<OakWBuffer> computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in computeIfPresent");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking(c, lookUp)) {
                    continue;
                }
                ValueUtils.ValueResult res = valueOperator.compute(lookUp.valueSlice, computer, lookUp.version);
                if (res == TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already marked as deleted, continue to construct another slice
                    return true;
                } else if (res == RETRY) {
                    continue;
                }
            }
            return false;
        }
    }

    LookUp getValueFromIndex(long keyReference) {
        K deserializedKey = keySerializer.deserialize(getKeyByteBuffer(keyReference));
        while (true) {
            Chunk<K, V> c = findChunk(deserializedKey); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(deserializedKey);
            if (lookUp == null || lookUp.valueSlice == null) {
                return null;
            }

            if (updateVersionAfterLinking(c, lookUp)) {
                continue;
            }
            return lookUp;
        }
    }

    private <T> T getValueTransformation(ByteBuffer key, Function<ByteBuffer, T> transformer) {
        K deserializedKey = keySerializer.deserialize(key);
        return getValueTransformation(deserializedKey, transformer);
    }

    <T> T getValueTransformation(K key, Function<ByteBuffer, T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in non-zc get");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp == null || lookUp.valueSlice == null) {
                return null;
            }

            if (updateVersionAfterLinking(c, lookUp)) {
                continue;
            }
            Result<T> res = valueOperator.transform(lookUp.valueSlice, transformer, lookUp.version);
            if (res.operationResult == RETRY) {
                continue;
            }
            return res.value;
        }

    }

    <T> T getKeyTransformation(K key, Function<ByteBuffer, T> transformer) {
        ByteBuffer serializedKey = getKey(key);
        if (serializedKey == null) {
            return null;
        }
        return transformer.apply(serializedKey.slice().asReadOnlyBuffer());
    }

    private ByteBuffer getKey(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key);
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null || lookUp.entryIndex == INVALID_ENTRY_INDEX) {
            return null;
        }
        return c.readKey(lookUp.entryIndex);
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

    // encapsulates finding of the chunk in the skip list and later chunk list traversal
    private Chunk<K, V> findChunk(K key) {
        Chunk<K, V> c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);
        return c;
    }

    V replace(K key, V value, Function<ByteBuffer, V> valueDeserializeTransformer) {
        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in replace");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp == null || lookUp.valueSlice == null) {
                return null;
            }

            // will return null if the value is deleted
            Result<V> result = valueOperator.exchange(c, lookUp, value, valueDeserializeTransformer, valueSerializer,
                    memoryManager);
            if (result.operationResult != RETRY) {
                return result.value;
            }
        }
    }

    boolean replace(K key, V oldValue, V newValue, Function<ByteBuffer, V> valueDeserializeTransformer) {
        int infiLoopCount = 0;
        while (true) {
            infiLoopCount++;
            if (infiLoopCount > 1000) {
                System.out.println("Stuck in compare exchange");
                assert false;
            }
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp == null || lookUp.valueSlice == null) {
                return false;
            }

            ValueUtils.ValueResult res = valueOperator.compareExchange(c, lookUp, oldValue, newValue,
                    valueDeserializeTransformer, valueSerializer, memoryManager);
            if (res == RETRY) {
                continue;
            }
            return res == TRUE;
        }
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
            int cmp = comparator.compareKeyAndSerializedKey(key, c.readKey(nextIndex));
            if (cmp <= 0) {
                break;
            }
            prevIndex = nextIndex;
        }

        /* Edge case: we're looking for the lowest key in the map and it's still greater than minkey
            (in which  case prevKey == key) */
        ByteBuffer prevKey = c.readKey(prevIndex);
        if (comparator.compareKeyAndSerializedKey(key, prevKey) == 0) {
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }
        K keyDeserialized = keySerializer.deserialize(prevKey.slice());

        int[] valueVersion = new int[1];
        long valueReference = c.getValueReferenceAndVersion(prevIndex, valueVersion);
        if (valueReference == INVALID_VALUE_REFERENCE) {
            return lowerEntry(key);
        }
        Result<V> valueDeserialized = valueOperator.transform(getValueSlice(valueReference),
                valueSerializer::deserialize,
                valueVersion[0]);
        if (valueDeserialized.operationResult != TRUE) {
            return lowerEntry(key);
        }
        return new AbstractMap.SimpleImmutableEntry<>(keyDeserialized, valueDeserialized.value);
    }

    void createImmutableIndex() {

        Iter iter = new Iter(null, true, null, true, false) {
            @Override public Integer next() {
                IteratorState state = advanceStateOnly();
                Chunk c = state.getChunk();
                int entryIndex = state.getIndex();
                ByteBuffer bbKey = c.readKey(entryIndex);
                // copy entry from given chunk 'c', entry at index 'entryIndex'
                // to entriesHash at index 'keySerializedHash(bbKey)'
                int hashIndex = keySerializer.serializedHash(bbKey);
                if (hashIndex < 0) {  // user didn't implemented a hash, or implemented it wrongly
                  return -1;
                }
                // each integer index need to be transformed to the index in the "entries array",
                // meaning need to skipp all integers of one entry and add 1 for the header integer
                //hashIndex = (hashIndex*ENTRIES_FIELDS+1)%(entriesHash.length-ENTRIES_FIELDS);
                // 1. limit hash within the addresses possible in the entriesHash
                hashIndex = hashIndex%((entriesHash.length-1)/ENTRIES_FIELDS);
                // 2. Get to correct address: skip in ENTRIES_FIELDS steps and add for header
                hashIndex = hashIndex*ENTRIES_FIELDS+1;
                //hashIndex = (hashIndex%((entriesHash.length-1)/ENTRIES_FIELDS))+1;
                for(int i=0; i<ENTRIES_FIELDS; i++) {
                    // if there are hash collisions the last one will be the "winner"
                    entriesHash[hashIndex+i]=c.entries[entryIndex+i];
                }
                return 0;
            }
        };

        entriesHash = new int[size.get()* ENTRIES_FIELDS + ENTRIES_FIRST_ITEM];
        int cnt=0;
        while(iter.hasNext()) {
           iter.next();
           cnt++;
        }
        System.out.println("\nHash was created going over " + cnt + " entries.\n");
    }

    /*-------------- Iterators --------------*/

    private Slice getValueSlice(long valuerReference) {
        if (valuerReference == INVALID_VALUE_REFERENCE) {
            return null;
        }
        int[] valueArray = longToInts(valuerReference);
        return memoryManager.getSliceFromBlockID(valueArray[BLOCK_ID_LENGTH_ARRAY_INDEX] >>> VALUE_BLOCK_SHIFT,
                valueArray[POSITION_ARRAY_INDEX], valueArray[BLOCK_ID_LENGTH_ARRAY_INDEX] & VALUE_LENGTH_MASK);
    }

    private ByteBuffer getKeyByteBuffer(long keyReference) {
        int[] keyArray = longToInts(keyReference);
        return memoryManager.getByteBufferFromBlockID(keyArray[BLOCK_ID_LENGTH_ARRAY_INDEX] >>> KEY_BLOCK_SHIFT,
                keyArray[POSITION_ARRAY_INDEX], keyArray[BLOCK_ID_LENGTH_ARRAY_INDEX] & KEY_LENGTH_MASK);
    }

    private OakRReference setKeyReference(long keyReference, OakRReference key) {
        int[] keyArray = longToInts(keyReference);
        int blockID = keyArray[BLOCK_ID_LENGTH_ARRAY_INDEX] >> KEY_BLOCK_SHIFT;
        int keyPosition = keyArray[POSITION_ARRAY_INDEX];
        int length = keyArray[BLOCK_ID_LENGTH_ARRAY_INDEX] & KEY_LENGTH_MASK;

        key.setReference(blockID, keyPosition, length);
        return key;
    }

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


        static <K, V> IteratorState<K, V> newInstance(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter) {
            return new IteratorState<>(nextChunk, nextChunkIter, Chunk.NONE);
        }

    }

    private static class IterItem {
        long keyReference;
        long valueReference;
        int valueVersion;
    }

    // This array holds for each thread a container instance so Iter::advance() could return the keyRefernece,
    // valueReference and version Reference, without the need to allocate any objects.
    private final IterItem[] iterItemsDummies = new IterItem[ThreadIndexCalculator.MAX_THREADS];
    private final ThreadIndexCalculator iterThreadIndexCalculator = ThreadIndexCalculator.newInstance();

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
            if (lo != null && hi != null && comparator.compare(lo, hi) > 0) {
                throw new IllegalArgumentException("inconsistent range");
            }

            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            this.isDescending = isDescending;

            initState(isDescending, lo, loInclusive, hi, hiInclusive);

        }

        boolean tooLow(ByteBuffer key) {
            int c;
            return (lo != null && ((c = comparator.compareKeyAndSerializedKey(lo, key)) > 0 ||
                    (c == 0 && !loInclusive)));
        }

        boolean tooHigh(ByteBuffer key) {
            int c;
            return (hi != null && ((c = comparator.compareKeyAndSerializedKey(hi, key)) < 0 ||
                    (c == 0 && !hiInclusive)));
        }


        boolean inBounds(ByteBuffer key) {
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
                // There are no more elements in Oak after nextKey, so throw NoSuchElementException
                throw new NoSuchElementException();
            }
        }


        // the actual next()
        abstract public T next();

        /**
         * Advances next to higher entry.
         * Return previous index
         *
         * @return The first long is the key's reference, the integer is the value's version and the second long is
         * the value's reference. If {@code needsValue == false}, then the value of the map entry is {@code null}.
         */
        IterItem advance(boolean needsValue) {
            if (iterItemsDummies[iterThreadIndexCalculator.getIndex()] == null) {
                iterItemsDummies[iterThreadIndexCalculator.getIndex()] = new IterItem();
            }
            IterItem myItem = iterItemsDummies[iterThreadIndexCalculator.getIndex()];
            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }

            long keyReference = state.getChunk().getKeyReference(state.getIndex());
            long valueReference;
            int[] valueVersion = new int[1];
            if (needsValue) {
                valueReference = state.getChunk().getValueReferenceAndVersion(state.getIndex(), valueVersion);
                if (valueReference != INVALID_VALUE_REFERENCE) {
                    valueVersion[0] = state.getChunk().completeLinking(new LookUp(null, valueReference,
                            state.getIndex(), valueVersion[0]));
                    // The CAS could not complete due to concurrent rebalance, so rebalance and try again
                    if (valueVersion[0] == INVALID_VERSION) {
                        rebalance(state.getChunk());
                        return advance(true);
                    }
                    // If we could not complete the linking or if the value is deleted, advance to the next value
                    if (valueOperator.isValueDeleted(getValueSlice(valueReference), valueVersion[0]) != FALSE) {
                        advanceState();
                        return advance(true);
                    }
                } else {
                    advanceState();
                    return advance(true);
                }
                myItem.valueReference = valueReference;
                myItem.valueVersion = valueVersion[0];
            }
            advanceState();
            myItem.keyReference = keyReference;
            return myItem;
        }

        /**
         * Advances next to the next entry without creating a ByteBuffer for the key.
         * Return previous index
         */
        OakRReference advanceStream(OakRReference key, OakRReference value) {

            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }
            if (key != null) {
                state.getChunk().setKeyRefer(state.getIndex(), key);
            }
            // if there a reference to update (this if is not executed for KeyStreamIterator)
            if (value != null) {
                if (!state.getChunk().setValueRefer(state.getIndex(), value)) {
                    // If the current value is deleted, then advance and try again
                    advanceState();
                    return advanceStream(key, value);
                }
            }
            advanceState();
            return value;
        }

        /**
         * Advances next to the next entry. Return previous index
         */
        IteratorState advanceStateOnly() {

            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }
            IteratorState currentState = IteratorState.newInstance(state.getChunk(), state.getChunkIter());
            currentState.set(state.getChunk(), state.getChunkIter(), state.getIndex());
            advanceState();
            return currentState;
        }

        private void initState(boolean isDescending, K lowerBound, boolean lowerInclusive,
                               K upperBound, boolean upperInclusive) {

            Chunk.ChunkIter nextChunkIter;
            Chunk<K, V> nextChunk;

            if (!isDescending) {
                if (lowerBound != null) {
                    nextChunk = skiplist.floorEntry(lowerBound).getValue();
                } else {
                    nextChunk = skiplist.floorEntry(minKey).getValue();
                }
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
            if ((hi != null && !isDescending) || (lo != null && isDescending)) {
                ByteBuffer key = state.getChunk().readKey(state.getIndex());
                if (!inBounds(key)) {
                    state = null;
                    return;
                }
            }
        }
    }

    class InternalIterator extends Iter<IteratorState> {

        InternalIterator() {
            super(null, true, null, true, false);
        }

        @Override
        public IteratorState next() {

            return null;
        }
    }

    class ValueIterator extends Iter<OakRBuffer> {

        private final InternalOakMap<K, V> internalOakMap;

        ValueIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        @Override
        public OakRBuffer next() {
            IterItem myItem = advance(true);
            long keyReference = myItem.keyReference;
            long valueReference = myItem.valueReference;
            int version = myItem.valueVersion;
            return new OakRValueBufferImpl(valueReference, version, keyReference, valueOperator, memoryManager,
                    internalOakMap);
        }
    }

    class ValueStreamIterator extends Iter<OakRBuffer> {

        private OakRReference value = new OakRReference(memoryManager, valueOperator.getHeaderSize());

        ValueStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {
            value = advanceStream(null, value);
            if (value == null) {
                return null;
            }
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
            IterItem myItem = advance(true);
            long keyReference = myItem.keyReference;
            long valueReference = myItem.valueReference;
            Slice valueSlice = getValueSlice(valueReference);
            int version = myItem.valueVersion;
            Result<T> res = valueOperator.transform(valueSlice, transformer, version);
            // If this value is deleted, try the next one
            if (res.operationResult == FALSE) {
                return next();
            }
            // if the value was moved, fetch it from the
            else if (res.operationResult == RETRY) {
                T result = getValueTransformation(getKeyByteBuffer(keyReference), transformer);
                if (result == null) {
                    // the value was deleted, try the next one
                    return next();
                }
                return result;
            }
            return res.value;
        }
    }

    class EntryIterator extends Iter<Map.Entry<OakRBuffer, OakRBuffer>> {

        private final InternalOakMap<K, V> internalOakMap;

        EntryIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        public Map.Entry<OakRBuffer, OakRBuffer> next() {
            IterItem myItem = advance(true);
            long keyReference = myItem.keyReference;
            long valueReference = myItem.valueReference;
            int version = myItem.valueVersion;
            return new AbstractMap.SimpleImmutableEntry<>(setKeyReference(keyReference,
                    new OakRReference(memoryManager, KEY_HEADER_SIZE)), new OakRValueBufferImpl(valueReference,
                    version, keyReference, valueOperator, memoryManager, internalOakMap));
        }
    }

    class EntryStreamIterator extends Iter<Map.Entry<OakRBuffer, OakRBuffer>> {

        private OakRReference key = new OakRReference(memoryManager, KEY_HEADER_SIZE);
        private OakRReference value = new OakRReference(memoryManager, valueOperator.getHeaderSize());

        EntryStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        public Map.Entry<OakRBuffer, OakRBuffer> next() {
            value = advanceStream(key, value);
            if (value == null) {
                return null;
            }
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
            IterItem myItem = advance(true);
            long keyReference = myItem.keyReference;
            long valueReference = myItem.valueReference;
            Slice valueSlice = getValueSlice(valueReference);
            int version = myItem.valueVersion;
            assert valueSlice != null;
            ValueUtils.ValueResult res = valueOperator.lockRead(valueSlice, version);
            ByteBuffer serializedValue;
            if (res == FALSE) {
                return next();
            } else if (res == RETRY) {
                do {
                    LookUp lookUp = getValueFromIndex(keyReference);
                    if (lookUp == null || lookUp.valueSlice == null) {
                        return next();
                    }
                    res = valueOperator.lockRead(lookUp.valueSlice, lookUp.version);
                    if (res == TRUE) {
                        valueReference = lookUp.valueReference;
                        valueSlice = lookUp.valueSlice;
                        version = lookUp.version;
                        break;
                    }
                } while (true);
            }
            serializedValue = valueOperator.getValueByteBufferNoHeader(valueSlice).asReadOnlyBuffer();
            Map.Entry<ByteBuffer, ByteBuffer> entry =
                    new AbstractMap.SimpleEntry<>(getKeyByteBuffer(keyReference).asReadOnlyBuffer(), serializedValue);

            T transformation = transformer.apply(entry);
            valueSlice = getValueSlice(valueReference);
            valueOperator.unlockRead(valueSlice, version);
            return transformation;
        }
    }

    // May return deleted keys
    class KeyIterator extends Iter<OakRBuffer> {

        KeyIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {

            IterItem myItem = advance(false);
            return setKeyReference(myItem.keyReference, new OakRReference(memoryManager, KEY_HEADER_SIZE));

        }
    }

    public class KeyStreamIterator extends Iter<OakRBuffer> {

        private OakRReference key = new OakRReference(memoryManager, KEY_HEADER_SIZE);

        KeyStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {
            advanceStream(key, null);
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
            IterItem myItem = advance(false);
            return transformer.apply(getKeyByteBuffer(myItem.keyReference).asReadOnlyBuffer());
        }
    }

    // Factory methods for iterators

    Iterator<OakRBuffer> valuesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                  boolean isDescending) {
        return new ValueIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesBufferViewIterator(K lo, boolean loInclusive, K hi,
                                                                          boolean hiInclusive, boolean isDescending) {
        return new EntryIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<OakRBuffer> keysBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                boolean isDescending) {
        return new KeyIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakRBuffer> valuesStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                              boolean isDescending) {
        return new ValueStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesStreamIterator(K lo, boolean loInclusive, K hi,
                                                                      boolean hiInclusive, boolean isDescending) {
        return new EntryStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakRBuffer> keysStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                            boolean isDescending) {
        return new KeyStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    <T> Iterator<T> valuesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                            boolean isDescending, Function<ByteBuffer, T> transformer) {
        return new ValueTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> entriesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                             boolean isDescending,
                                             Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
        return new EntryTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> keysTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                                          Function<ByteBuffer, T> transformer) {
        return new KeyTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

}
