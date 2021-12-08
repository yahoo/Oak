/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
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
    private final AtomicReference<OrderedChunk<K, V>> nextToEngage;
    private final AtomicReference<List<OrderedChunk<K, V>>> newChunks = new AtomicReference<>(null);
    private final AtomicReference<List<OrderedChunk<K, V>>> engagedChunks = new AtomicReference<>(null);
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private final OrderedChunk<K, V> first;
    private OrderedChunk<K, V> last;
    private int chunksInRange;
    private int itemsInRange;

    /*-------------- Constructors --------------*/

    Rebalancer(OrderedChunk<K, V> orderedChunk) {
        this.entriesLowThreshold = (int) (orderedChunk.array.entryCount() * LOW_THRESHOLD);
        this.maxRangeToAppend = (int) (orderedChunk.array.entryCount() * APPEND_THRESHOLD);
        this.maxAfterMergeItems = (int) (orderedChunk.array.entryCount() * MAX_AFTER_MERGE_PART);
        nextToEngage = new AtomicReference<>(orderedChunk);
        this.first = orderedChunk;
        last = orderedChunk;
        chunksInRange = 1;
        itemsInRange = first.getStatistics().getTotalCount();
    }

    /*-------------- Methods --------------*/

    Rebalancer<K, V> engageChunks() {
        while (true) {
            OrderedChunk<K, V> next = nextToEngage.get();
            if (next == null) {
                break;
            }

            next.engage(this);
            if (!next.isEngaged(this) && next == first) {
                // the first chunk was engage by a different rebalancer, help it
                return next.getRebalancer().engageChunks();
            }

            OrderedChunk<K, V> candidate = findNextCandidate();

            // if fail to CAS here, another thread has updated next candidate
            // continue to while loop and try to engage it
            nextToEngage.compareAndSet(next, candidate);
        }
        updateRangeView();

        List<OrderedChunk<K, V>> engaged = createEngagedList();

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

        for (OrderedChunk<K, V> orderedChunk : getEngagedChunks()) {
            orderedChunk.freeze();
        }

        frozen.set(true);
    }

    /**
     * Split or compact
     *
     * @return if managed to CAS to newChunk list of rebalance
     * if we did then the put was inserted
     */
    boolean createNewChunks(ThreadContext ctx) {

        if (this.newChunks.get() != null) {
            return false; // this was done by another thread already
        }

        List<OrderedChunk<K, V>> frozenOrderedChunks = engagedChunks.get();

        ListIterator<OrderedChunk<K, V>> iterFrozen = frozenOrderedChunks.listIterator();

        OrderedChunk<K, V> firstFrozen = iterFrozen.next();
        OrderedChunk<K, V> currFrozen = firstFrozen;
        OrderedChunk<K, V> currNewOrderedChunk = firstFrozen.createFirstChild();

        int ei = firstFrozen.getHeadNextEntryIndex();
        List<OrderedChunk<K, V>> newOrderedChunks = new LinkedList<>();

        KeyBuffer keyBuff = ctx.tempKey;
        ValueBuffer valueBuff = ctx.tempValue;

        while (true) {
            ei = currNewOrderedChunk
                .copyPartOfEntries(valueBuff, currFrozen, ei, entriesLowThreshold);
            // if completed reading curr frozen chunk
            if (ei == OrderedChunk.NONE_NEXT) {
                if (!iterFrozen.hasNext()) {
                    break;
                }

                currFrozen = iterFrozen.next();
                ei = currFrozen.getHeadNextEntryIndex();

            } else { // filled new chunk up to entriesLowThreshold

                List<OrderedChunk<K, V>> frozenSuffix = frozenOrderedChunks
                    .subList(iterFrozen.previousIndex(), frozenOrderedChunks.size());
                // try to look ahead and add frozen suffix
                if (canAppendSuffix(frozenSuffix, maxRangeToAppend)) {
                    // maybe there is just a little bit copying left
                    // and we don't want to open a whole new chunk just for it
                    completeCopy(valueBuff, currNewOrderedChunk, ei, frozenSuffix);
                    break;
                } else {
                    // we have to open an new chunk
                    // here we create a new minimal key buffer for the second new chunk,
                    // created by the split. The new min key is a copy of the older one

                    currFrozen.readKey(keyBuff, ei);
                    OrderedChunk<K, V> c = firstFrozen.createNextChild(keyBuff);
                    currNewOrderedChunk.next.set(c, false);
                    newOrderedChunks.add(currNewOrderedChunk);
                    currNewOrderedChunk = c;
                }
            }

        }

        newOrderedChunks.add(currNewOrderedChunk);

        // if fail here, another thread succeeded, and op is effectively gone
        return this.newChunks.compareAndSet(null, newOrderedChunks);
    }

    private boolean canAppendSuffix(List<OrderedChunk<K, V>> frozenSuffix, int maxCount) {
        Iterator<OrderedChunk<K, V>> iter = frozenSuffix.iterator();
        // first of frozen chunks already have entriesLowThreshold copied into new one
        boolean firstChunk = true;
        int counter = 0;
        // use statistics to find out how much is left to copy
        while (iter.hasNext() && counter < maxCount) {
            OrderedChunk<K, V> c = iter.next();
            counter += c.getStatistics().getTotalCount();
            if (firstChunk) {
                counter -= entriesLowThreshold;
                firstChunk = false;
            }
        }
        return counter < maxCount;
    }

    private void completeCopy(
        ValueBuffer tempValue, OrderedChunk<K, V> dest,
        final int ei, List<OrderedChunk<K, V>> srcOrderedChunks) {

        final int maxItems = dest.array.entryCount();
        Iterator<OrderedChunk<K, V>> iter = srcOrderedChunks.iterator();

        OrderedChunk<K, V> src = iter.next();
        dest.copyPartOfEntries(tempValue, src, ei, maxItems);

        while (iter.hasNext()) {
            OrderedChunk<K, V> curSrc = iter.next();
            int curEntryIndex = src.getHeadNextEntryIndex();
            dest.copyPartOfEntries(tempValue, curSrc, curEntryIndex, maxItems);
        }
    }

    private OrderedChunk<K, V> findNextCandidate() {

        updateRangeView();

        // allow up to RebalanceSize chunks to be engaged
        if (chunksInRange >= REBALANCE_SIZE) {
            return null;
        }

        OrderedChunk<K, V> candidate = last.next.getReference();

        if (!isCandidate(candidate)) {
            return null;
        }

        int newItems = candidate.getStatistics().getTotalCount();
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
            OrderedChunk<K, V> next = last.next.getReference();
            if (next == null || !next.isEngaged(this)) {
                break;
            }
            last = next;
            addToCounters(last);
        }
    }

    private void addToCounters(OrderedChunk<K, V> orderedChunk) {
        itemsInRange += orderedChunk.getStatistics().getTotalCount();
        chunksInRange++;
    }

    /***
     * verifies that the orderedChunk is not engaged and not null
     * @param orderedChunk candidate orderedChunk for range extension
     * @return true if not engaged and not null
     */
    private boolean isCandidate(OrderedChunk<K, V> orderedChunk) {
        // do not take chunks that are engaged with another rebalancer or infant
        return orderedChunk != null && orderedChunk.isEngaged(null) && (
            orderedChunk.state() != BasicChunk.State.INFANT) &&
                (orderedChunk.state() != BasicChunk.State.RELEASED);
    }

    private List<OrderedChunk<K, V>> createEngagedList() {
        OrderedChunk<K, V> current = first;
        List<OrderedChunk<K, V>> engaged = new LinkedList<>();

        while (current != null && current.isEngaged(this)) {
            engaged.add(current);
            current = current.next.getReference();
        }

        if (engaged.isEmpty()) {
            throw new IllegalStateException("Engaged list cannot be empty");
        }

        return engaged;
    }

    List<OrderedChunk<K, V>> getEngagedChunks() {
        List<OrderedChunk<K, V>> engaged = engagedChunks.get();
        if (engaged == null) {
            throw new IllegalStateException("Trying to get engaged before engagement stage completed");
        }
        return engaged;
    }

    List<OrderedChunk<K, V>> getNewChunks() {
        List<OrderedChunk<K, V>> newOrderedChunks = this.newChunks.get();
        if (newOrderedChunks == null) {
            throw new IllegalStateException("Trying to get new chunks before creating stage completed");
        }
        return newOrderedChunks;
    }

}
