/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;


abstract class InternalOakBasics<K, V> {
    /*-------------- Members --------------*/
    protected static final int MAX_RETRIES = 1024;


    protected final OakSharedConfig<K, V> config;

    /*-------------- Constructors --------------*/
    InternalOakBasics(OakSharedConfig<K, V> config) {
        this.config = config;
    }

    /*-------------- Closable --------------*/
    /**
     * cleans only off heap memory
     */
    void close() {
        try {
            // closing the same memory manager (or memory allocator) twice,
            // has the same effect as closing once
            config.valuesMemoryManager.close();
            config.keysMemoryManager.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*-------------- size --------------*/
    /**
     * @return current off heap memory usage in bytes
     */
    long memorySize() {
        if (config.valuesMemoryManager != config.keysMemoryManager) {
            // Two memory managers are not the same instance, but they
            // may still have the same allocator and allocator defines how many bytes are allocated
            if (config.valuesMemoryManager.getBlockMemoryAllocator()
                != config.keysMemoryManager.getBlockMemoryAllocator()) {
                return config.valuesMemoryManager.allocated() + config.keysMemoryManager.allocated();
            }
        }
        return config.valuesMemoryManager.allocated();
    }

    int entries() {
        return config.size.get();
    }

    /* getter methods */
    public MemoryManager getValuesMemoryManager() {
        return config.valuesMemoryManager;
    }

    public MemoryManager getKeysMemoryManager() {
        return config.keysMemoryManager;
    }

    protected OakSerializer<K> getKeySerializer() {
        return config.keySerializer;
    }

    protected OakSerializer<V> getValueSerializer() {
        return config.valueSerializer;
    }

    protected ValueUtils getValueOperator() {
        return config.valueOperator;
    }

    /*-------------- Context --------------*/
    /**
     * Should only be called from API methods at the beginning of the method and be reused in internal calls.
     *
     * @return a context instance.
     */
    ThreadContext getThreadContext() {
        return new ThreadContext(config);
    }

    /*-------------- REBALANCE --------------*/
    /**
    * Tunneling for a specific chunk rebalance to be implemented in concrete internal map or hash
    * */
    protected abstract void rebalanceBasic(BasicChunk<K, V> c);

    protected void checkRebalance(BasicChunk<K, V> c) {
        if (c.shouldRebalance()) {
            rebalanceBasic(c);
        }
    }

    protected void helpRebalanceIfInProgress(BasicChunk<K, V> c) {
        if (c.state() == BasicChunk.State.FROZEN) {
            rebalanceBasic(c);
        }
    }

    protected boolean inTheMiddleOfRebalance(BasicChunk<K, V> c) {
        BasicChunk.State state = c.state();
        if (state == BasicChunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalanceBasic(c.creator());
            return true;
        }
        if (state == BasicChunk.State.FROZEN || state == BasicChunk.State.RELEASED) {
            rebalanceBasic(c);
            return true;
        }
        return false;
    }

    /*-------------- API methods --------------*/
    abstract V put(K key, V value, OakTransformer<V> transformer);

    /**
     * put the value associated with the key, only if key didn't exist
     * returned results describes whether the value was inserted or not
     * @param key key to check
     * @param value value to put if the key not present
     * @param transformer transformer to apply
     * @return returned results describes whether the value was inserted or not
     */
    abstract Result putIfAbsent(K key, V value, OakTransformer<V> transformer);

    /**
     * if key with a valid value exists in the map, apply compute function on the value
     * return true if compute did happen
     * @param key key for the operation
     * @param computer the operation to be performed
     * @return true, if the operation was performed
     */
    // moved internally to be able to catch DeletedMemAccess
    abstract boolean computeIfPresent(K key, Consumer<OakScopedWriteBuffer> computer);

    // moved internally to be able to catch DeletedMemAccess
    abstract Result remove(K key, V oldValue, OakTransformer<V> transformer); 

    // the zero-copy version of get
    abstract OakUnscopedBuffer get(K key);

    /*-------------- Common actions --------------*/
    /**
     * The method returns reference to the chunk the given key belongs to
     * @param key the key to look for in the chunks
     * @param ctx auxiliary parameter, used in some look up function
     * @return reference to the chunk the supplied key belongs to
     */
    protected abstract BasicChunk<K, V> findChunk(K key, ThreadContext ctx);

    V replace(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            BasicChunk<K, V> chunk = findChunk(key, ctx); // find Chunk matching key
            try {
                chunk.lookUp(ctx, key);
                //the look up method might encounter a chunk which is released, while using Nova as a memory manager
                //as a result we might access an already deleted key, thus the need to catch the exception 
            } catch (DeletedMemoryAccessException e) {
                assert !(chunk instanceof  HashChunk); 
                //Hash deals with deleted keys in an earlier stage
                continue;
            }
            if (!ctx.isValueValid()) {
                return null;
            }

            // will return null if the value is deleted
            Result result = config.valueOperator.exchange(chunk, ctx, value,
                    valueDeserializeTransformer, getValueSerializer());
            if (result.operationResult != ValueUtils.ValueResult.RETRY) {
                return (V) result.value;
            }
            // it might be that this chunk is proceeding with rebalance -> help
            helpRebalanceIfInProgress(chunk);
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    abstract boolean replace(K key, V oldValue, V newValue, OakTransformer<V> valueDeserializeTransformer);

    abstract boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakScopedWriteBuffer> computer);

    protected boolean finalizeDeletion(BasicChunk<K, V> c, ThreadContext ctx) {
        if (c.finalizeDeletion(ctx)) {
            rebalanceBasic(c);
            return true;
        }
        return false;
    }

    protected boolean isAfterRebalanceOrValueUpdate(BasicChunk<K, V> c, ThreadContext ctx) {
        // If orderedChunk is frozen or infant, can't proceed with put, need to help rebalancer first,
        // rebalance is done as part of inTheMiddleOfRebalance.
        // Also, if value is off-heap deleted, we need to finalizeDeletion on-heap, which can
        // cause rebalance as well. If rebalance happened finalizeDeletion returns true.
        // After rebalance we need to restart.
        if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, ctx)) {
            return true;
        }

        // Value can be valid again, if key was found and partially deleted value needed help.
        // But in the meanwhile value was reset to be another, valid value.
        // In Hash case value will be always invalid in the context, but the changes will be caught
        // during next entry allocation
        return ctx.isValueValid();
    }

    /**
     * See {@code refreshValuePosition(ctx)} for more details.
     *
     * @param key   the key to refresh
     * @param value the output value to update
     * @return true if the refresh was successful.
     */
    boolean refreshValuePosition(KeyBuffer key, ValueBuffer value) {
        ThreadContext ctx = getThreadContext();
        ctx.key.copyFrom(key);
        boolean isSuccessful = refreshValuePosition(ctx);

        if (!isSuccessful) {
            return false;
        }

        value.copyFrom(ctx.value);
        return true;
    }

    /**
     * Used when value of a key was possibly moved, and we try to search for the given key
     * through the OakMap again.
     *
     * @param ctx The context key should be initialized with the key to refresh, and the context value
     *            will be updated with the refreshed value.
     * @return true if the refresh was successful.
     */
    abstract boolean refreshValuePosition(ThreadContext ctx);


    protected <T> T getValueTransformation(OakScopedReadBuffer key, OakTransformer<T> transformer) {
        K deserializedKey = config.keySerializer.deserialize(key);
        return getValueTransformation(deserializedKey, transformer);
    }

    // the non-ZC variation of the get
    abstract <T> T getValueTransformation(K key, OakTransformer<T> transformer);

    /*-------------- Different Oak Buffer creations --------------*/

    protected UnscopedBuffer<KeyBuffer> getKeyUnscopedBuffer(ThreadContext ctx) {
        return new UnscopedBuffer<>(new KeyBuffer(ctx.key));
    }

    protected UnscopedValueBufferSynced getValueUnscopedBuffer(ThreadContext ctx) {
        return new UnscopedValueBufferSynced(ctx.key, ctx.value, this);
    }

    // Iterator State base class
    static class BasicIteratorState<K, V> {

        private BasicChunk<K, V> chunk;
        private BasicChunk.BasicChunkIter chunkIter;
        private int index;

        public void set(BasicChunk<K, V> chunk, BasicChunk.BasicChunkIter chunkIter, int index) {
            this.chunk = chunk;
            this.chunkIter = chunkIter;
            this.index = index;
        }

        protected BasicIteratorState(BasicChunk<K, V> nextChunk, BasicChunk.BasicChunkIter nextChunkIter,
                                     int nextIndex) {

            this.chunk = nextChunk;
            this.chunkIter = nextChunkIter;
            this.index = nextIndex;
        }

        BasicChunk<K, V> getChunk() {
            return chunk;
        }

        BasicChunk.BasicChunkIter getChunkIter() {
            return chunkIter;
        }

        public int getIndex() {
            return index;
        }

        public void copyState(BasicIteratorState<K, V> other) {
            assert other != null;
            this.chunk = other.chunk;
            this.chunkIter = other.chunkIter;
            this.index = other.index;
        }
    }


    /************************
    * Basic Iterator class
    *************************/
    abstract class BasicIter<T> implements Iterator<T> {


        /**
         * the next node to return from next();
         */
        private BasicIteratorState<K, V> state;
        private BasicIteratorState<K, V> prevIterState;
        private boolean prevIterStateValid = false;
        /**
         * An iterator cannot be accesses concurrently by multiple threads.
         * Thus, it is safe to have its own thread context.
         */
        protected ThreadContext ctx;

        /**
         * Initializes ascending iterator for entire range.
         */
        BasicIter() {
            this.ctx = new ThreadContext(config);
        }

        public boolean hasNext() {
            return (state != null);
        }

        // for more detail on this method see implementation 
        protected abstract void initStateWithMinKey(BasicChunk<K, V> chunk);

        protected abstract void initAfterRebalance();

        // the actual next()
        public abstract T next();

        /**
         * The function removes the element returned by the last call to next() function
         * If the next() was not called, exception is thrown
         * If the entry was changed, between the call of the next() and remove(), it is deleted regardless
         */
        @Override
        public void remove() {
            //@TODO under the current implementation, if the chunk was released between the call to the nex()
            // @TODO and the call to remove, the function behaviour is unpredictable

            if (!isPrevIterStateValid()) {
                throw new IllegalStateException("next() was not called in due order");
            }
            BasicChunk<K, V> prevChunk = getPrevIterState().getChunk();
            int preIdx = getPrevIterState().getIndex();
            boolean validState = prevChunk.readKeyFromEntryIndex(ctx.key, preIdx);
            if (validState) {
                K prevKey = getKeySerializer().deserialize(ctx.key);
                InternalOakBasics.this.remove(prevKey, null, null);
            }

            invalidatePrevState();
        }
        /**
         * Advances next to higher entry.
         *  previous index
         *
         * The first long is the key's reference, the integer is the value's version and the second long is
         * the value's reference. If {@code needsValue == false}, then the value of the map entry is {@code null}.
         */
        void advance(boolean needsValue) {
            boolean validState = false;

            while (!validState) {
                if (state == null) {
                    throw new NoSuchElementException();
                }

                final BasicChunk<K, V> chunk = state.getChunk();
                if (chunk.state() == BasicChunk.State.RELEASED) {

                    // @TODO not to access the keys on the RELEASED chunk once the key might be released
                    initAfterRebalance();
                    continue;
                }

                final int curIndex = state.getIndex();

                // build the entry context that sets key references and does not check for value validity.
                ctx.initEntryContext(curIndex);


                chunk.readKey(ctx);

                validState = ctx.isKeyValid();

                if (validState & needsValue) {
                    // Set value references and checks for value validity.
                    // if value is deleted ctx.entryState is going to be invalid
                    chunk.readValue(ctx);
                    validState = ctx.isValueValid();
                }

                advanceState();
            }
        }

        /**
         * Advances next to the next entry without creating a ByteBuffer for the key.
         * Return previous index
         */
        abstract void advanceStream(UnscopedBuffer<KeyBuffer> key, UnscopedBuffer<ValueBuffer> value);


        protected BasicIteratorState<K, V> getState() {
            return state;
        }

        protected void setState(BasicIteratorState<K, V> newState) {
            state = newState;
        }

        protected void setPrevState(BasicIteratorState<K, V> newState) {
            prevIterState = newState;
        }

        protected BasicIteratorState<K, V> getPrevIterState() {
            return prevIterState;
        }
        /**
         * function copies the fields of the current iterator state to the fields of the pre.IterState
         * This is used by the remove function of the iterator
         */
        protected void updatePreviousState() {
            prevIterState.copyState(state);
            prevIterStateValid = true;
        }

        protected boolean isPrevIterStateValid() {
            return prevIterStateValid;
        }

        /**
         * previous state should be invalidated to prevent invoking remove on the same entry twice
         */
        protected void invalidatePrevState() {
            prevIterStateValid = false;
        }

        protected abstract BasicChunk<K, V> getNextChunk(BasicChunk<K, V> current);

        protected abstract BasicChunk.BasicChunkIter getChunkIter(BasicChunk<K, V> current)
                throws DeletedMemoryAccessException;

        /**
         * advance state to the new position
         * @return if new position found, return true, else, set State to null and return false
         */
        protected boolean advanceState() {

            BasicChunk<K, V> chunk = getState().getChunk();
            BasicChunk.BasicChunkIter chunkIter = getState().getChunkIter();

            updatePreviousState();
            
            while (!chunkIter.hasNext()) { // skip empty chunks
                chunk = getNextChunk(chunk);
                if (chunk == null) {
                    //End of iteration
                    setState(null);
                    return false;
                }
                try {
                    chunkIter = getChunkIter(chunk);
                } catch (DeletedMemoryAccessException e) {
                    assert chunk.state.get() ==  BasicChunk.State.RELEASED;
                    initStateWithMinKey(chunk); 
                    return true;
                }
            }

            int nextIndex = chunkIter.next(ctx);
            getState().set(chunk, chunkIter, nextIndex);

            return true; 
        }

    }
}



