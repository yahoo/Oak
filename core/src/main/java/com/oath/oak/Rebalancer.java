/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class Rebalancer<K, V> {


    /*-------------- Constants --------------*/

    private final int rebalanceSize;
    private final double maxAfterMergePart;
    private final double lowThreshold;
    private final double appendThreshold;
    private final int entriesLowThreshold;
    private final int keyBytesLowThreshold;
    private final int maxRangeToAppend;
    private final int maxBytesToAppend;
    private final int maxAfterMergeItems;
    private final int maxAfterMergeBytes;

    /*-------------- Members --------------*/
    private final ThreadIndexCalculator threadIndexCalculator;
    private final AtomicReference<Chunk> nextToEngage;
    private final AtomicReference<List<Chunk<K,V>>> newChunks = new AtomicReference<>(null);
    private final AtomicReference<List<Chunk<K,V>>> engagedChunks = new AtomicReference<>(null);
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private final Chunk<K, V> first;
    private Chunk<K, V> last;
    private int chunksInRange;
    private int itemsInRange;
    private int bytesInRange;
    private final Comparator<Object> comparator;
    private final boolean offHeap;
    private final MemoryManager memoryManager;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;

    /*-------------- Constructors --------------*/

    Rebalancer(Chunk<K, V> chunk, Comparator<Object> comparator, boolean offHeap, MemoryManager memoryManager,
               OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
               ThreadIndexCalculator threadIndexCalculator) {
        this.rebalanceSize = 2;
        this.maxAfterMergePart = 0.7;
        this.lowThreshold = 0.5;
        this.appendThreshold = 0.2;
        this.entriesLowThreshold = (int) (chunk.getMaxItems() * this.lowThreshold);
        this.keyBytesLowThreshold = (int) (chunk.getMaxItems() * chunk.getBytesPerItem() * this.lowThreshold);
        this.maxRangeToAppend = (int) (chunk.getMaxItems() * this.appendThreshold);
        this.maxBytesToAppend = (int) (chunk.getMaxItems() * chunk.getBytesPerItem() * this.appendThreshold);
        this.maxAfterMergeItems = (int) (chunk.getMaxItems() * this.maxAfterMergePart);
        this.maxAfterMergeBytes = (int) (chunk.getMaxItems() * chunk.getBytesPerItem() * this.maxAfterMergePart);

        this.comparator = comparator;
        this.offHeap = offHeap;
        this.memoryManager = memoryManager;
        nextToEngage = new AtomicReference<>(chunk);
        this.first = chunk;
        last = chunk;
        chunksInRange = 1;
        itemsInRange = first.getStatistics().getCompactedCount();
        bytesInRange = first.keyIndex.get();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.threadIndexCalculator = threadIndexCalculator;
    }

    static class RebalanceResult {
        final boolean success;
        final Handle oldHandle;     // non-null handle means someone helped with insertion

        RebalanceResult(boolean success, Handle handle) {
            this.success = success;
            this.oldHandle = handle;
        }
    }

    /*-------------- Methods --------------*/

    /**
     * compares ByteBuffer by calling the provided comparator
     */
    private int compare(Object k1, Object k2) {
        return comparator.compare(k1, k2);
    }

    Rebalancer<K,V> engageChunks() {
        while (true) {
            Chunk next = nextToEngage.get();
            if (next == null) {
                break;
            }

            next.engage(this);
            if (!next.isEngaged(this) && next == first) {
                // the first chunk was engage by a different rebalancer, help it
                return next.getRebalancer().engageChunks();
            }

            Chunk candidate = findNextCandidate();

            // if fail to CAS here, another thread has updated next candidate
            // continue to while loop and try to engage it
            nextToEngage.compareAndSet(next, candidate);
        }
        updateRangeView();

        List<Chunk<K,V>> engaged = createEngagedList();

        engagedChunks.compareAndSet(null, engaged); // if CAS fails here - another thread has updated it

        return this;
    }

    /**
     * Freeze the engaged chunks. Should be called after engageChunks.
     * Marks chunks as freezed, prevents future updates of the engagead chunks
     */
    void freeze() {
        if (frozen.get()) return;

        for (Chunk chunk : getEngagedChunks()) {
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
    RebalanceResult createNewChunks() {

        assert offHeap;
        if (this.newChunks.get() != null) {
            return new RebalanceResult(false, null); // this was done by another thread already
        }

        List<Chunk<K,V>> frozenChunks = engagedChunks.get();

        ListIterator<Chunk<K,V>> iterFrozen = frozenChunks.listIterator();

        Chunk<K,V> firstFrozen = iterFrozen.next();
        Chunk<K,V> currFrozen = firstFrozen;
        Chunk<K,V> currNewChunk = new Chunk<K, V>(firstFrozen.minKey, firstFrozen, firstFrozen.comparator, memoryManager,
                currFrozen.getMaxItems(), currFrozen.getBytesPerItem(), currFrozen.externalSize,
                keySerializer, valueSerializer, threadIndexCalculator);

        int ei = firstFrozen.getFirstItemEntryIndex();

        List<Chunk<K,V>> newChunks = new LinkedList<>();

        while (true) {
            ei = currNewChunk.copyPart(currFrozen, ei, entriesLowThreshold, keyBytesLowThreshold);
            // if completed reading curr frozen chunk
            if (ei == Chunk.NONE) {
                if (!iterFrozen.hasNext())
                    break;

                currFrozen = iterFrozen.next();
                ei = currFrozen.getFirstItemEntryIndex();

            } else { // filled new chunk up to ENETRIES_LOW_THRESHOLD

                List<Chunk<K,V>> frozenSuffix = frozenChunks.subList(iterFrozen.previousIndex(), frozenChunks.size());
                // try to look ahead and add frozen suffix
                if (canAppendSuffix(frozenSuffix, maxRangeToAppend, maxBytesToAppend)) {
                    // maybe there is just a little bit copying left
                    // and we don't want to open a whole new chunk just for it
                    completeCopy(currNewChunk, ei, frozenSuffix);
                    break;
                } else {
                    // we have to open an new chunk
                    // TODO do we want to use slice here?
                    ByteBuffer bb = currFrozen.readKey(ei).slice();
                    int remaining = bb.remaining();
                    int position = bb.position();
                    ByteBuffer newMinKey = ByteBuffer.allocate(remaining);
                    int myPos = newMinKey.position();
                    for (int i = 0; i < remaining; i++) {
                        newMinKey.put(myPos + i, bb.get(i + position));
                    }
                    newMinKey.rewind();
                    Chunk c = new Chunk<K, V>(newMinKey, firstFrozen, currFrozen.comparator, memoryManager,
                            currFrozen.getMaxItems(), currFrozen.getBytesPerItem(), currFrozen.externalSize,
                            keySerializer, valueSerializer, threadIndexCalculator);
                    currNewChunk.next.set(c, false);
                    newChunks.add(currNewChunk);
                    currNewChunk = c;
                }
            }

        }

        newChunks.add(currNewChunk);

        // if fail here, another thread succeeded, and op is effectively gone
        boolean cas = this.newChunks.compareAndSet(null, newChunks);
        return new RebalanceResult(cas, null);
    }

    private boolean canAppendSuffix(List<Chunk<K,V>> frozenSuffix, int maxCount, int maxBytes) {
        Iterator<Chunk<K,V>> iter = frozenSuffix.iterator();
        int counter = 0;
        int bytesSum = 0;
        // use statistics to find out how much is left to copy
        while (iter.hasNext() && counter < maxCount && bytesSum < maxBytes) {
            Chunk<K,V> c = iter.next();
            counter += c.getStatistics().getCompactedCount();
            bytesSum += c.keyIndex.get();
        }
        return counter < maxCount && bytesSum < maxBytes;
    }

    private void completeCopy(Chunk<K,V> dest, int ei, List<Chunk<K,V>> srcChunks) {
        Iterator<Chunk<K,V>> iter = srcChunks.iterator();
        Chunk<K,V> src = iter.next();
        int maxItems = src.getMaxItems();
        int maxKeyBytes = maxItems * src.getBytesPerItem();
        dest.copyPart(src, ei, maxItems, maxKeyBytes);
        while (iter.hasNext()) {
            src = iter.next();
            ei = src.getFirstItemEntryIndex();
            dest.copyPart(src, ei, maxItems, maxKeyBytes);
        }
    }

    private Chunk<K,V> findNextCandidate() {

        updateRangeView();

        // allow up to RebalanceSize chunks to be engaged
        if (chunksInRange >= rebalanceSize) return null;

        Chunk<K,V> candidate = last.next.getReference();

        if (!isCandidate(candidate)) return null;

        int newItems = candidate.getStatistics().getCompactedCount();
        int newBytes = candidate.keyIndex.get();
        int totalItems = itemsInRange + newItems;
        int totalBytes = bytesInRange + newBytes;
        // TODO think if this makes sense
        int chunksAfterMerge = Math.max((int) Math.ceil(((double) totalItems) / maxAfterMergeItems),
                (int) Math.ceil(((double) totalBytes) / maxAfterMergeBytes));

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
            if (next == null || !next.isEngaged(this)) break;
            last = next;
            addToCounters(last);
        }
    }

    private void addToCounters(Chunk chunk) {
        itemsInRange += chunk.getStatistics().getCompactedCount();
        chunksInRange++;
        bytesInRange += chunk.keyIndex.get();
    }

    /***
     * verifies that the chunk is not engaged and not null
     * @param chunk candidate chunk for range extension
     * @return true if not engaged and not null
     */
    private boolean isCandidate(Chunk chunk) {
        // do not take chunks that are engaged with another rebalancer or infant
        return chunk != null && chunk.isEngaged(null) && (chunk.state() != Chunk.State.INFANT) && (chunk.state() != Chunk.State.RELEASED);
    }

    private List<Chunk<K,V>> createEngagedList() {
        Chunk<K, V> current = first;
        List<Chunk<K,V>> engaged = new LinkedList<>();

        while (current != null && current.isEngaged(this)) {
            engaged.add(current);
            current = current.next.getReference();
        }

        if (engaged.isEmpty()) throw new IllegalStateException("Engaged list cannot be empty");

        return engaged;
    }

    List<Chunk<K,V>> getEngagedChunks() {
        List<Chunk<K,V>> engaged = engagedChunks.get();
        if (engaged == null) throw new IllegalStateException("Trying to get engaged before engagement stage completed");
        return engaged;
    }

    List<Chunk<K,V>> getNewChunks() {
        List<Chunk<K,V>> newChunks = this.newChunks.get();
        if (newChunks == null)
            throw new IllegalStateException("Trying to get new chunks before creating stage completed");
        return newChunks;
    }

}
