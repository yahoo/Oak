/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class BasicChunk<K, V> {

    /*-------------- Constants --------------*/
    enum State {
        INFANT,
        NORMAL,
        FROZEN,
        RELEASED
    }

    /*-------------- Members --------------*/
    // to compare serilized and object keys
    protected OakComparator<K> comparator;
    // in split/compact process, represents parent of split (can be null!)
    private final AtomicReference<BasicChunk<K, V>> creator;
    // chunk can be in the following states: normal, frozen or infant(has a creator)
    private final AtomicReference<State> state;
    private final AtomicReference<Rebalancer<K, V>> rebalancer;
    private final AtomicInteger pendingOps;
    private final int maxItems;
    protected AtomicInteger externalSize; // for updating oak's size (reference to one global per Oak size)
    protected final Statistics statistics;

    /*-------------- Constructors and creators --------------*/
    /**
     * This constructor is only used internally to instantiate a BasicChunk without a creator and a state.
     * The caller should set the creator and state before returning the BasicChunk to the user.
     */
    protected BasicChunk(int maxItems, AtomicInteger externalSize, OakComparator<K> comparator) {
        this.maxItems = maxItems;
        this.externalSize = externalSize;
        this.comparator = comparator;
        this.creator = new AtomicReference<>(null);
        this.state = new AtomicReference<>(State.NORMAL);
        this.pendingOps = new AtomicInteger();
        this.rebalancer = new AtomicReference<>(null); // to be updated on rebalance
        this.statistics = new Statistics();
    }

    /**
     * Create a child BasicChunk where this BasicChunk object as its creator.
     */
    protected void updateBasicChild(BasicChunk<K, V> child) {
        child.creator.set(this);
        child.state.set(State.INFANT);
        return;
    }

    /*-------------- Publishing related methods and getters ---------------*/
    /**
     * publish operation so rebalance will wait for it
     * if CAS didn't succeed then this means that a rebalancer got here first and chunk is frozen
     *
     * @return result of CAS
     **/
    boolean publish() {
        pendingOps.incrementAndGet();
        State currentState = state.get();
        if (currentState == State.FROZEN || currentState == State.RELEASED) {
            pendingOps.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * un-publish operation
     * if CAS didn't succeed then this means that a rebalancer did this already
     **/
    void unpublish() {
        pendingOps.decrementAndGet();
    }

    int getMaxItems() {
        return maxItems;
    }

    /*------------------------- Methods that are used for rebalance  ---------------------------*/
    /**
     * Engage the chunk to a rebalancer r.
     *
     * @param r -- a rebalancer to engage with
     */
    void engage(Rebalancer<K, V> r) {
        rebalancer.compareAndSet(null, r);
    }

    /**
     * Checks whether the chunk is engaged with a given rebalancer.
     *
     * @param r -- a rebalancer object. If r is null, verifies that the chunk is not engaged to any rebalancer
     * @return true if the chunk is engaged with r, false otherwise
     */
    boolean isEngaged(Rebalancer<K, V> r) {
        return rebalancer.get() == r;
    }

    /**
     * Fetch a rebalancer engaged with the chunk.
     *
     * @return rebalancer object or null if not engaged.
     */
    Rebalancer<K, V> getRebalancer() {
        return rebalancer.get();
    }

    /*----------------------- Methods for managing the chunk's state  --------------------------*/
    /* To normalize the chunk once its split/merge/rebalance is finished */
    void normalize() {
        state.compareAndSet(State.INFANT, State.NORMAL);
        creator.set(null);
        // using fence so other puts can continue working immediately on this chunk
        UnsafeUtils.UNSAFE.storeFence();
    }

    State state() {
        return state.get();
    }

    BasicChunk<K, V> creator() {
        return creator.get();
    }

    private void setState(State state) {
        this.state.set(state);
    }

    /**
     * freezes chunk so no more changes can be done to it (marks pending items as frozen)
     */
    void freeze() {
        setState(State.FROZEN); // prevent new puts to this chunk
        while (pendingOps.get() != 0) {
            assert Boolean.TRUE;
        }
    }

    /**
     * try to change the state from frozen to released
     */
    void release() {
        state.compareAndSet(State.FROZEN, State.RELEASED);
    }

    /*-------------- Statistics --------------*/
    /**
     * This class contains information about chunk utilization.
     */
    static class Statistics {
        private final AtomicInteger addedCount = new AtomicInteger(0);
        private int initialCount = 0;

        /**
         * Initial sorted count here is immutable after chunk re-balance
         */
        void updateInitialCount(int sortedCount) {
            this.initialCount = sortedCount;
        }

        /**
         * @return number of items chunk will contain after compaction.
         */
        int getTotalCount() {
            return initialCount + addedCount.get();
        }

        /**
         * Incremented when put a key that was removed before
         */
        void incrementAddedCount() {
            addedCount.incrementAndGet();
        }

        /**
         * Decrement when remove a key that was put before
         */
        void decrementAddedCount() {
            addedCount.decrementAndGet();
        }

    }

    /**
     * @return statistics object containing approximate utilization information.
     */
    Statistics getStatistics() {
        return statistics;
    }
}
