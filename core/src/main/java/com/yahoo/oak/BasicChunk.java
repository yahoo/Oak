/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

abstract class BasicChunk<K, V> {

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
    protected KeyMemoryManager kMM;
    // in split/compact process, represents parent of split (can be null!)
    private final AtomicReference<BasicChunk<K, V>> creator;
    // chunk can be in the following states: normal, frozen or infant(has a creator)
    protected final AtomicReference<State> state;
    private final AtomicReference<Rebalancer<K, V>> rebalancer;
    private final AtomicInteger pendingOps;
    private final int maxItems;
    protected final boolean releaseKeys;
    protected AtomicInteger externalSize; // for updating oak's size (reference to one global per Oak size)
    protected final Statistics statistics;

    /*-------------- Constructors and creators --------------*/
    /**
     * This constructor is only used internally to instantiate a BasicChunk without a creator and a state.
     * The caller should set the creator and state before returning the BasicChunk to the user.
     */
    protected BasicChunk(int maxItems, AtomicInteger externalSize, OakComparator<K> comparator, KeyMemoryManager kMM) {
        this.maxItems = maxItems;
        this.externalSize = externalSize;
        this.comparator = comparator;
        this.kMM = kMM;
        this.creator = new AtomicReference<>(null);
        this.state = new AtomicReference<>(State.NORMAL);
        this.pendingOps = new AtomicInteger();
        this.releaseKeys = ! (kMM instanceof SeqExpandMemoryManager);
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

    /*----------------------- Abstract Rebalance-related Methods  --------------------------*/
    /**
     * Check whether this better to be rebalanced (not a necessary). Necessary rebalance is
     * triggered anyway regardless to this method.
     * */
    abstract boolean shouldRebalance();

    /*----------------------- Abstract Mappings-related Methods  --------------------------*/
    /**
     * See concrete implementation for more information
     */
    abstract void releaseKey(ThreadContext ctx);

    /**
     * See concrete implementation for more information
     */
    abstract void releaseNewValue(ThreadContext ctx);

    /**
     * Writes the key off-heap and allocates an entry with the reference pointing to the given key
     * See concrete implementation for more information
     */
    abstract boolean allocateEntryAndWriteKey(ThreadContext ctx, K key);

    /**
     * See concrete implementation for more information
     */
    abstract void allocateValue(ThreadContext ctx, V value, boolean writeForMove);

    /**
     * This function does the physical CAS of the value reference, which is the
     * Linearization Point of the insertion (for any chunk) to complete the insertion
     *
     * @param ctx The context that follows the operation since the key was found/created.
     *            Holds the entry to which the value reference is linked, the old and new value references and
     *            the old and new value versions.
     * @return true if the value reference was CASed successfully.
     */
    abstract ValueUtils.ValueResult linkValue(ThreadContext ctx);

    /**
     * When deleting an entry the first step is marking (CAS) the delete bit in the value's off-heap
     * header, this is the Linearization Point (LP) of deletion.
     * Later series of steps need to be performed to accomplish the entry deletion.
     * This method accomplish the rest of the algorithm coming after LP.
     * Other threads seeing a marked value call this function before they proceed (e.g.,
     * before performing a successful {@code putIfAbsent()}).
     *
     * @param ctx The context that follows the operation since the key was found/created.
     *            Holds the entry to change, the old key/value reference to CAS out,
     *            and the current value version.
     * @return true if a rebalance is needed
     * IMPORTANT: whether deleteValueFinish succeeded to mark the entry's value reference as
     * deleted, or not, if there were no request to rebalance FALSE is going to be returned
     */
    abstract boolean finalizeDeletion(ThreadContext ctx);

    /*-------------- Methods for managing existing value (for ValueUtils) --------------*/
    boolean overwriteExistingValueForMove(ThreadContext ctx, V newVal) {
        // given old entry index (inside ctx) and new value, while old value is locked,
        // allocate new value, new value is going to be locked as well, write the new value
        allocateValue(ctx, newVal, true);

        // in order to connect/overwrite the old entry to point to new value
        // we need to publish as in the normal write process
        if (!publish()) {
            releaseNewValue(ctx);
            return false;
        }

        // updating the old entry index
        if (linkValue(ctx) != ValueUtils.ValueResult.TRUE) {
            releaseNewValue(ctx);
            unpublish();
            return false;
        }

        unpublish();
        return true;
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
