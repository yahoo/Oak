/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class Rebalancer<K, V> {


    /*-------------- Constants --------------*/

    private static final int REBALANCE_SIZE = 2;
    private static final double MAX_AFTER_MERGE_PART = 0.7;
    private static final double LOW_THRESHOLD = 0.5;
    private static final double APPEND_THRESHOLD = 0.2;

    private final int entriesLowThreshold;
    private final int maxRangeToAppend;
    private final int maxAfterMergeItems;

    /*-------------- Members --------------*/
    private final AtomicReference<Chunk<K, V>> nextToEngage;
    private final AtomicReference<List<Chunk<K, V>>> newChunks = new AtomicReference<>(null);
    private final AtomicReference<List<Chunk<K, V>>> engagedChunks = new AtomicReference<>(null);
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private final Chunk<K, V> first;
    private Chunk<K, V> last;
    private int chunksInRange;
    private int itemsInRange;
    private final boolean offHeap;
    private final NovaManager memoryManager;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    private final NovaValueOperations operator;

    /*-------------- Constructors --------------*/

    Rebalancer(Chunk<K, V> chunk, boolean offHeap, NovaManager memoryManager,
               OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer, NovaValueOperations operator) {
        this.entriesLowThreshold = (int) (chunk.getMaxItems() * LOW_THRESHOLD);
        this.maxRangeToAppend = (int) (chunk.getMaxItems() * APPEND_THRESHOLD);
        this.maxAfterMergeItems = (int) (chunk.getMaxItems() * MAX_AFTER_MERGE_PART);
        this.offHeap = offHeap;
        this.memoryManager = memoryManager;
        nextToEngage = new AtomicReference<>(chunk);
        this.first = chunk;
        last = chunk;
        chunksInRange = 1;
        itemsInRange = first.getStatistics().getCompactedCount();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.operator = operator;
    }

    /*-------------- Methods --------------*/

    Rebalancer<K, V> engageChunks() {
        while (true) {
            Chunk<K, V> next = nextToEngage.get();
            if (next == null) {
                break;
            }

            next.engage(this);
            if (!next.isEngaged(this) && next == first) {
                // the first chunk was engage by a different rebalancer, help it
                return next.getRebalancer().engageChunks();
            }

            Chunk<K, V> candidate = findNextCandidate();

            // if fail to CAS here, another thread has updated next candidate
            // continue to while loop and try to engage it
            nextToEngage.compareAndSet(next, candidate);
        }
        updateRangeView();

        List<Chunk<K, V>> engaged = createEngagedList();

        engagedChunks.compareAndSet(null, engaged); // if CAS fails here - another thread has updated it

        return this;
    }

    /**
     * Freeze the engaged chunks. Should be called after engageChunks.
     * Marks chunks as frozen, prevents future updates of the engaged chunks
     */
    void freeze() {
        if (frozen.get()) {
            return;
        }

        for (Chunk<K, V> chunk : getEngagedChunks()) {
            chunk.freeze();
        }

        frozen.set(true);
    }

    /**
     * Split or compact
     *
     * @return if managed to CAS to newChunk list of rebalance
     * if we did then the put was inserted
     */
    boolean createNewChunks() {

        assert offHeap;
        if (this.newChunks.get() != null) {
            return false; // this was done by another thread already
        }

        List<Chunk<K, V>> frozenChunks = engagedChunks.get();

        ListIterator<Chunk<K, V>> iterFrozen = frozenChunks.listIterator();

        Chunk<K, V> firstFrozen = iterFrozen.next();
        Chunk<K, V> currFrozen = firstFrozen;
        Chunk<K, V> currNewChunk = new Chunk<>(firstFrozen.minKey, firstFrozen, firstFrozen.comparator, memoryManager,
                currFrozen.getMaxItems(), currFrozen.externalSize, keySerializer, valueSerializer, operator);

        int ei = firstFrozen.getFirstItemEntryIndex();
        List<Chunk<K, V>> newChunks = new LinkedList<>();

        while (true) {
            ei = currNewChunk.copyPartNoKeys(currFrozen, ei, entriesLowThreshold);
            // if completed reading curr frozen chunk
            if (ei == Chunk.NONE) {
                if (!iterFrozen.hasNext()) {
                    break;
                }

                currFrozen = iterFrozen.next();
                ei = currFrozen.getFirstItemEntryIndex();

            } else { // filled new chunk up to entriesLowThreshold

                List<Chunk<K, V>> frozenSuffix = frozenChunks.subList(iterFrozen.previousIndex(), frozenChunks.size());
                // try to look ahead and add frozen suffix
                if (canAppendSuffix(frozenSuffix, maxRangeToAppend)) {
                    // maybe there is just a little bit copying left
                    // and we don't want to open a whole new chunk just for it
                    completeCopy(currNewChunk, ei, frozenSuffix);
                    break;
                } else {
                    // we have to open an new chunk
                    // here we create a new on-heap minimal key for the second new chunk,
                    // created by the split. The new min key is on-heap copy of the one off-heap
                    // We need to use slice() method here as we want new object to be created
                    ByteBuffer bb = currFrozen.readKey(ei).slice();
                    int remaining = bb.remaining();
                    int position = bb.position();
                    ByteBuffer newMinKey = ByteBuffer.allocate(remaining);
                    int myPos = newMinKey.position();
                    for (int i = 0; i < remaining; i++) {
                        newMinKey.put(myPos + i, bb.get(i + position));
                    }
                    newMinKey.rewind();


                    Chunk<K, V> c = new Chunk<>(newMinKey, firstFrozen, currFrozen.comparator, memoryManager,
                            currFrozen.getMaxItems(), currFrozen.externalSize,
                            keySerializer, valueSerializer, operator);
                    currNewChunk.next.set(c, false);
                    newChunks.add(currNewChunk);
                    currNewChunk = c;
                }
            }

        }

        newChunks.add(currNewChunk);

        // if fail here, another thread succeeded, and op is effectively gone
        return this.newChunks.compareAndSet(null, newChunks);
    }

    private boolean canAppendSuffix(List<Chunk<K, V>> frozenSuffix, int maxCount) {
        Iterator<Chunk<K, V>> iter = frozenSuffix.iterator();
        // first of frozen chunks already have entriesLowThreshold copied into new one
        boolean firstChunk = true;
        int counter = 0;
        // use statistics to find out how much is left to copy
        while (iter.hasNext() && counter < maxCount) {
            Chunk<K, V> c = iter.next();
            counter += c.getStatistics().getCompactedCount();
            if (firstChunk) {
                counter -= entriesLowThreshold;
                firstChunk = false;
            }
        }
        return counter < maxCount;
    }

    private void completeCopy(Chunk<K, V> dest, int ei, List<Chunk<K, V>> srcChunks) {
        Iterator<Chunk<K, V>> iter = srcChunks.iterator();
        Chunk<K, V> src = iter.next();
        int maxItems = src.getMaxItems();
        dest.copyPartNoKeys(src, ei, maxItems);
        while (iter.hasNext()) {
            src = iter.next();
            ei = src.getFirstItemEntryIndex();
            dest.copyPartNoKeys(src, ei, maxItems);
        }
    }

    private Chunk<K, V> findNextCandidate() {

        updateRangeView();

        // allow up to RebalanceSize chunks to be engaged
        if (chunksInRange >= REBALANCE_SIZE) {
            return null;
        }

        Chunk<K, V> candidate = last.next.getReference();

        if (!isCandidate(candidate)) {
            return null;
        }

        int newItems = candidate.getStatistics().getCompactedCount();
        int totalItems = itemsInRange + newItems;
        // TODO think if this makes sense
        int chunksAfterMerge = (int) Math.ceil(((double) totalItems) / maxAfterMergeItems);

        // if the chosen chunk may reduce the number of chunks -- return it as candidate
        if (chunksAfterMerge < chunksInRange + 1) {
            return candidate;
        } else {
            return null;
        }
    }

    private void updateRangeView() {
        while (true) {
            Chunk<K, V> next = last.next.getReference();
            if (next == null || !next.isEngaged(this)) {
                break;
            }
            last = next;
            addToCounters(last);
        }
    }

    private void addToCounters(Chunk<K, V> chunk) {
        itemsInRange += chunk.getStatistics().getCompactedCount();
        chunksInRange++;
    }

    /***
     * verifies that the chunk is not engaged and not null
     * @param chunk candidate chunk for range extension
     * @return true if not engaged and not null
     */
    private boolean isCandidate(Chunk<K, V> chunk) {
        // do not take chunks that are engaged with another rebalancer or infant
        return chunk != null && chunk.isEngaged(null) && (chunk.state() != Chunk.State.INFANT) && (chunk.state() != Chunk.State.RELEASED);
    }

    private List<Chunk<K, V>> createEngagedList() {
        Chunk<K, V> current = first;
        List<Chunk<K, V>> engaged = new LinkedList<>();

        while (current != null && current.isEngaged(this)) {
            engaged.add(current);
            current = current.next.getReference();
        }

        if (engaged.isEmpty()) {
            throw new IllegalStateException("Engaged list cannot be empty");
        }

        return engaged;
    }

    List<Chunk<K, V>> getEngagedChunks() {
        List<Chunk<K, V>> engaged = engagedChunks.get();
        if (engaged == null) {
            throw new IllegalStateException("Trying to get engaged before engagement stage completed");
        }
        return engaged;
    }

    List<Chunk<K, V>> getNewChunks() {
        List<Chunk<K, V>> newChunks = this.newChunks.get();
        if (newChunks == null) {
            throw new IllegalStateException("Trying to get new chunks before creating stage completed");
        }
        return newChunks;
    }

}
