/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.EmptyStackException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class Chunk<K, V> {

    /*-------------- Constants --------------*/

    static final int NONE = 0;    // constant for "no index", etc. MUST BE 0!

    // location of the first (head) node - just a next pointer
    private static final int HEAD_NODE = 0;
    // index of first item in array, after head (not necessarily first in list!)
    private static final int FIRST_ITEM = 1;

    private static final int FIELDS = 4;  // # of fields in each item of key array
    private static final int OFFSET_NEXT = 0;
    private static final int OFFSET_KEY_POSITION = 1;
    private static final int OFFSET_KEY_LENGTH = 2;
    private static final int OFFSET_HANDLE_INDEX = 3;
    // key block is not used as an offset, rather as request differentiation,
    // key block is part of key length integer, thus key length is limited to 65KB
    private static final int OFFSET_KEY_BLOCK = 4;
    private static final int KEY_LENGTH_MASK = 0xffff; // 16 lower bits
    private static final int KEY_BLOCK_SHIFT = 16;

    // used for checking if rebalance is needed
    private static final double REBALANCE_PROB_PERC = 30;
    private static final double SORTED_REBALANCE_RATIO = 2;
    private static final double MAX_ENTRIES_FACTOR = 2;
    private static final double MAX_IDLE_ENTRIES_FACTOR = 5;

    // when chunk is frozen, all of the elements in pending puts array will be this OpData
    private static final OpData FROZEN_OP_DATA =
            new OpData(Operation.NO_OP, 0, 0, 0, null);

    // defaults
    public static final int MAX_ITEMS_DEFAULT = 4096;

    private static final Unsafe unsafe;
    private final MemoryManager memoryManager;
    ByteBuffer minKey;       // minimal key that can be put in this chunk
    AtomicMarkableReference<Chunk<K, V>> next;
    Comparator<Object> comparator;

    // in split/compact process, represents parent of split (can be null!)
    private AtomicReference<Chunk<K, V>> creator;
    // chunk can be in the following states: normal, frozen or infant(has a creator)
    private final AtomicReference<State> state;
    private AtomicReference<Rebalancer> rebalancer;
    private final int[] entries;    // array is initialized to 0, i.e., NONE - this is important!

    private final Handle<V>[] handles;
    private AtomicInteger pendingOps;
    private final AtomicInteger entryIndex;    // points to next free index of entry array
    private final AtomicInteger handleIndex;   // points to next free index of entry array
    private final Statistics statistics;
    // # of sorted items at entry-array's beginning (resulting from split)
    private AtomicInteger sortedCount;
    private final int maxItems;
    AtomicInteger externalSize; // for updating oak's size
    // for writing the keys into the bytebuffers
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;

    /*-------------- Constructors --------------*/

    // static constructor - access and create a new instance of Unsafe
    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create a new chunk
     *
     * @param minKey                minimal key to be placed in chunk
     * @param creator               the chunk that is responsible for this chunk creation
     * @param threadIndexCalculator
     */
    Chunk(ByteBuffer minKey, Chunk creator, Comparator<Object> comparator, MemoryManager memoryManager,
          int maxItems, AtomicInteger externalSize, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
          ThreadIndexCalculator threadIndexCalculator) {
        this.memoryManager = memoryManager;
        this.maxItems = maxItems;
        this.entries = new int[maxItems * FIELDS + FIRST_ITEM];
        this.entryIndex = new AtomicInteger(FIRST_ITEM);
        this.handles = new Handle[maxItems + FIRST_ITEM];
        this.handleIndex = new AtomicInteger(FIRST_ITEM);

        this.sortedCount = new AtomicInteger(0);
        this.minKey = minKey;
        this.creator = new AtomicReference<>(creator);
        if (creator == null)
            this.state = new AtomicReference<>(State.NORMAL);
        else
            this.state = new AtomicReference<>(State.INFANT);
        this.next = new AtomicMarkableReference<>(null, false);
        this.pendingOps = new AtomicInteger();
        this.rebalancer = new AtomicReference<>(null); // to be updated on rebalance
        this.statistics = new Statistics();
        this.comparator = comparator;
        this.externalSize = externalSize;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    enum State {
        INFANT,
        NORMAL,
        FROZEN,
        RELEASED
    }

    static class OpData {
        final Operation op;
        final int entryIndex;
        final int handleIndex;
        int prevHandleIndex;
        final Consumer<OakWBuffer> computer;

        OpData(Operation op, int entryIndex, int handleIndex, int prevHandleIndex, Consumer<OakWBuffer> computer) {
            this.op = op;
            this.entryIndex = entryIndex;
            this.handleIndex = handleIndex;
            this.prevHandleIndex = prevHandleIndex;
            this.computer = computer;
        }
    }

    /*-------------- Methods --------------*/

    void release() {
        // try to change the state
        state.compareAndSet(State.FROZEN, State.RELEASED);
    }

    /**
     * compares ByteBuffer by calling the provided comparator
     */
    private int compare(Object k1, Object k2) {
        return comparator.compare(k1, k2);
    }

    /**
     * performs CAS from 'expected' to 'value' for field at specified offset of given item in key array
     */
    private boolean casEntriesArray(int item, int offset, int expected, int value) {
        return unsafe.compareAndSwapInt(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expected, value);
    }


    /**
     * write key in slice
     **/
    void writeKey(K key, int ei) {
        int keySize = keySerializer.calculateSize(key);
        Slice s
                = memoryManager.allocateSlice(keySize);
        // byteBuffer.slice() is set so it protects us from the overwrites of the serializer
        keySerializer.serialize(key, s.getByteBuffer().slice());

        setEntryField(ei, OFFSET_KEY_BLOCK, s.getBlockID());
        setEntryField(ei, OFFSET_KEY_POSITION, s.getByteBuffer().position());
        setEntryField(ei, OFFSET_KEY_LENGTH, keySize);
    }

    /**
     * Reads a key given the entry index. Key is returned via reusable thread-local ByteBuffer.
     * There is no copy just a special ByteBuffer for a single key.
     * The thread-local ByteBuffer can be reused by different threads, however as long as
     * a thread is invoked the ByteBuffer is related solely to this thread.
     */
    ByteBuffer readKey(int entryIndex) {
        if (entryIndex == Chunk.NONE) {
            return null;
        }

        int blockID = getEntryField(entryIndex, OFFSET_KEY_BLOCK);
        int keyPosition = getEntryField(entryIndex, OFFSET_KEY_POSITION);
        int length = getEntryField(entryIndex, OFFSET_KEY_LENGTH);

        ByteBuffer bb = memoryManager.getByteBufferFromBlockID(blockID, keyPosition, length);
        return bb;
    }

    /**
     * release key in slice, currently not in use, waiting for GC to be arranged
     **/
    void releaseKey(int entryIndex) {
        int blockID = getEntryField(entryIndex, OFFSET_KEY_BLOCK);
        int keyPosition = getEntryField(entryIndex, OFFSET_KEY_POSITION);
        int length = getEntryField(entryIndex, OFFSET_KEY_LENGTH);
        ByteBuffer bb = memoryManager.getByteBufferFromBlockID(blockID, keyPosition, length);
        Slice s = new Slice(blockID, bb);

        memoryManager.releaseSlice(s);
    }

    ByteBuffer readMinKey() {
        int minEntry = getFirstItemEntryIndex();
        return readKey(minEntry);
    }

    ByteBuffer readMaxKey() {
        int maxEntry = getLastItemEntryIndex();
        return readKey(maxEntry);
    }

    /**
     * gets the field of specified offset for given item in entry array
     */
    private int getEntryField(int item, int offset) {
        switch (offset) {
            case OFFSET_KEY_LENGTH:
                // return two low bytes of the key length index int
                return (entries[item + OFFSET_KEY_LENGTH] & KEY_LENGTH_MASK);
            case OFFSET_KEY_BLOCK:
                // offset must be OFFSET_KEY_BLOCK, return 2 high bytes of the int inside key length
                // right-shift force, fill empty with zeroes
                return (entries[item + OFFSET_KEY_LENGTH] >>> KEY_BLOCK_SHIFT);
            default:
                return entries[item + offset];
        }
    }

    /**
     * * sets the field of specified offset to 'value' for given item in entry array
     */
    private void setEntryField(int item, int offset, int value) {
        assert item + offset >= 0;
        switch (offset) {
            case OFFSET_KEY_LENGTH:
                // OFFSET_KEY_LENGTH and OFFSET_KEY_BLOCK should be less then 16 bits long
                // *2 in order to get read of the signed vs unsigned limits
                assert value < Short.MAX_VALUE * 2;
                // set two low bytes of the handle index int
                entries[item + OFFSET_KEY_LENGTH] =
                        (entries[item + OFFSET_KEY_LENGTH]) | (value & KEY_LENGTH_MASK);
                return;
            case OFFSET_KEY_BLOCK:
                // OFFSET_KEY_LENGTH and OFFSET_KEY_BLOCK should be less then 16 bits long
                // *2 in order to get read of the signed vs unsigned limits
                assert value < Short.MAX_VALUE * 2;
                // offset must be OFFSET_KEY_BLOCK,
                // set 2 high bytes of the int inside OFFSET_KEY_LENGTH
                assert value > 0; // block id can never be 0
                entries[item + OFFSET_KEY_LENGTH] =
                        (entries[item + OFFSET_KEY_LENGTH]) | (value << KEY_BLOCK_SHIFT);
                return;
            default:
                entries[item + offset] = value;
                return;
        }
    }

    /**
     * gets the value for the given item, or 'null' if it doesn't exist
     */
    Handle<V> getHandle(int entryIndex) {

        int hi = getEntryField(entryIndex, OFFSET_HANDLE_INDEX);

        // if no value for item - return null
        assert hi != 0; // because we got to it from linked list
        if (hi < 0) {
            return null;
        } else {
            return handles[hi];
        }
    }

    int getHandleIndex(int entryIndex) {
        return getEntryField(entryIndex, OFFSET_HANDLE_INDEX);
    }

    /**
     * look up key
     */
    LookUp<V> lookUp(K key) {
        // binary search sorted part of key array to quickly find node to start search at
        // it finds previous-to-key so start with its next
        int curr = getEntryField(binaryFind(key), OFFSET_NEXT);
        int cmp;
        // iterate until end of list (or key is found)
        while (curr != NONE) {
            // compare current item's key to searched key
            cmp = compare(readKey(curr), key);
            // if item's key is larger - we've exceeded our key
            // it's not in chunk - no need to search further
            if (cmp > 0)
                return null;
                // if keys are equal - we've found the item
            else if (cmp == 0) {
                int hi = getEntryField(curr, OFFSET_HANDLE_INDEX);
                if (hi < 0) return new LookUp<>(null, curr, -1);
                Handle<V> h = handles[hi];
                if (h.isDeleted()) return new LookUp<>(null, curr, hi);
                return new LookUp<>(h, curr, hi);
            }
            // otherwise- proceed to next item
            else
                curr = getEntryField(curr, OFFSET_NEXT);
        }
        return null;
    }

    static class LookUp<V> {

        final Handle<V> handle;
        final int entryIndex;
        final int handleIndex;

        LookUp(Handle<V> handle, int entryIndex, int handleIndex) {
            this.handle = handle;
            this.entryIndex = entryIndex;
            this.handleIndex = handleIndex;
        }
    }


    /**
     * binary search for largest-entry smaller than 'key' in sorted part of key array.
     *
     * @return the index of the entry from which to start a linear search -
     * if key is found, its previous entry is returned!
     */
    private int binaryFind(K key) {
        int sortedCount = this.sortedCount.get();
        // if there are no sorted keys, or the first item is already larger than key -
        // return the head node for a regular linear search
        if ((sortedCount == 0) || compare(readKey(FIRST_ITEM), key) >= 0)
            return HEAD_NODE;

        // optimization: compare with last key to avoid binary search
        if (compare(readKey((sortedCount - 1) * FIELDS + FIRST_ITEM), key) < 0)
            return (sortedCount - 1) * FIELDS + FIRST_ITEM;

        int start = 0;
        int end = sortedCount;

        while (end - start > 1) {
            int curr = start + (end - start) / 2;

            if (compare(readKey(curr * FIELDS + FIRST_ITEM), key) >= 0)
                end = curr;
            else
                start = curr;
        }

        return start * FIELDS + FIRST_ITEM;
    }

    /**
     * publish operation into thread array
     * if CAS didn't succeed then this means that a rebalancer got here first and entry is frozen
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
     * unpublish operation from thread array
     * if CAS didn't succeed then this means that a rebalancer did this already
     **/
    void unpublish() {
        pendingOps.decrementAndGet();
    }

    /**
     * allocate value handle
     *
     * @return if chunk is full return -1, otherwise return new handle index
     */
    int allocateHandle() {
        int hi = handleIndex.getAndIncrement();
        if (hi + 1 > handles.length) {
            return -1;
        }
        handles[hi] = new Handle<>();
        return hi;
    }

    int allocateEntryAndKey(K key) {
        int ei = entryIndex.getAndAdd(FIELDS);
        if (ei + FIELDS > entries.length) {
            return -1;
        }

        // key and value must be set before linking to the list so it will make sense when reached before put is done
        setEntryField(ei, OFFSET_HANDLE_INDEX, -1); // set value index to -1, value is init to null
        writeKey(key, ei);
        return ei;
    }

    int linkEntry(int ei, boolean cas, K key) {
        int prev, curr, cmp;
        int anchor = -1;

        while (true) {
            // start iterating from quickly-found node (by binary search) in sorted part of order-array
            if (anchor == -1) anchor = binaryFind(key);
            curr = anchor;

            // iterate items until key's position is found
            while (true) {
                prev = curr;
                curr = getEntryField(prev, OFFSET_NEXT);    // index of next item in list

                // if no item, done searching - add to end of list
                if (curr == NONE) {
                    break;
                }
                // compare current item's key to ours
                cmp = compare(readKey(curr), key);

                // if current item's key is larger, done searching - add between prev and curr
                if (cmp > 0) {
                    break;
                }

                // if same key, someone else managed to add the key to the linked list
                if (cmp == 0) {
                    return curr;
                }
            }

            // link to list between next and previous
            // first change this key's next to point to curr
            setEntryField(ei, OFFSET_NEXT, curr); // no need for CAS since put is not even published yet
            if (cas) {
                if (casEntriesArray(prev, OFFSET_NEXT, curr, ei)) {
                    // Here is the single place where we do enter a new entry to the chunk, meaning
                    // there is none else simultaneously inserting the same key
                    // (we were the first to insert this key).
                    // If the new entry's index is exactly after the sorted count and
                    // the entry's key is greater or equal then to the previous (sorted count)
                    // index key. Then increase the sorted count.
                    int sortedCount = this.sortedCount.get();
                    if (sortedCount > 0) {
                        if (ei == (sortedCount * FIELDS + 1)) {
                            // the new entry's index is exactly after the sorted count
                            if (compare(
                                    readKey((sortedCount - 1) * FIELDS + FIRST_ITEM), key) <= 0) {
                                // compare with sorted count key, if inserting the "if-statement",
                                // the sorted count key is less or equal to the key just inserted
                                this.sortedCount.compareAndSet(sortedCount, (sortedCount + 1));
                            }
                        }
                    }
                    return ei;
                }
                // CAS didn't succeed, try again
            } else {
                // without CAS (used by rebalance)
                setEntryField(prev, OFFSET_NEXT, ei);
            }
        }
    }

    /**
     * write value in place
     **/
    void writeValue(int hi, V value) {
        assert hi >= 0;
        ByteBuffer byteBuffer = memoryManager.allocate(valueSerializer.calculateSize(value) + ValueUtils.VALUE_HEADER_SIZE);
        // initializing the header lock
        byteBuffer.putInt(byteBuffer.position(), 0);
        // just allocated byte buffer is ensured to have position 0
        // One duplication
        valueSerializer.serialize(value, ValueUtils.getActualValueBufferLessDuplications(byteBuffer));
        handles[hi].setValue(byteBuffer);
    }

    public int getMaxItems() {
        return maxItems;
    }

    /**
     * Updates a linked entry to point to handle or otherwise removes such a link. The handle in
     * turn has the value. For linkage this is an insert linearization point.
     * All the relevant data can be found inside opData.
     * <p>
     * if someone else got to it first (helping rebalancer or other operation), returns the old handle
     */
    Handle<V> pointToValue(OpData opData) {

        // try to perform the CAS according to operation data (opData)
        if (pointToValueCAS(opData, true)) {
            return null;
        }

        // the straight forward helping didn't work, check why
        Operation operation = opData.op;

        // the operation is remove, means we tried to change the handle index we knew about to -1
        // the old handle index is no longer there so we have nothing to do
        if (operation == Operation.REMOVE) {
            return null; // this is a remove, no need to try again and return doesn't matter
        }

        // the operation is either NO_OP, PUT, PUT_IF_ABSENT, COMPUTE
        int expectedHandleIdx = opData.handleIndex;
        int foundHandleIdx = getEntryField(opData.entryIndex, OFFSET_HANDLE_INDEX);

        if (foundHandleIdx == expectedHandleIdx) {
            return null; // someone helped
        } else if (foundHandleIdx < 0) {
            // the handle was deleted, retry the attach
            opData.prevHandleIndex = -1;
            return pointToValue(opData); // remove completed, try again
        } else if (operation == Operation.PUT_IF_ABSENT) {
            return handles[foundHandleIdx]; // too late
        } else if (operation == Operation.COMPUTE) {
            Handle<V> h = handles[foundHandleIdx];
            if (h != null) {
                boolean succ = h.compute(opData.computer);
                if (!succ) {
                    // we tried to perform the compute but the handle was deleted,
                    // we can get to pointToValue with Operation.COMPUTE only from PIACIP
                    // retry to make a put and to attach the new handle
                    opData.prevHandleIndex = foundHandleIdx;
                    return pointToValue(opData);
                }
            }
            return h;
        }
        // this is a put, try again
        opData.prevHandleIndex = foundHandleIdx;
        return pointToValue(opData);
    }

    /**
     * used by put/putIfAbsent/remove and rebalancer
     */
    boolean pointToValueCAS(OpData opData, boolean cas) {
        if (cas) {
            if (casEntriesArray(opData.entryIndex, OFFSET_HANDLE_INDEX, opData.prevHandleIndex, opData.handleIndex)) {
                // update statistics only by thread that CASed
                if (opData.prevHandleIndex < 0 && opData.handleIndex > 0) { // previously a remove
                    statistics.incrementAddedCount();
                    externalSize.incrementAndGet();
                } else if (opData.prevHandleIndex > 0 && opData.handleIndex == -1) { // removing
                    statistics.decrementAddedCount();
                    externalSize.decrementAndGet();
                }
                return true;
            }
        } else {
            setEntryField(opData.entryIndex, OFFSET_HANDLE_INDEX, opData.handleIndex);
        }
        return false;
    }

    /**
     * Engage the chunk to a rebalancer r.
     *
     * @param r -- a rebalancer to engage with
     */
    void engage(Rebalancer r) {
        rebalancer.compareAndSet(null, r);
    }

    /**
     * Checks whether the chunk is engaged with a given rebalancer.
     *
     * @param r -- a rebalancer object. If r is null, verifies that the chunk is not engaged to any rebalancer
     * @return true if the chunk is engaged with r, false otherwise
     */
    boolean isEngaged(Rebalancer r) {
        return rebalancer.get() == r;
    }

    /**
     * Fetch a rebalancer engaged with the chunk.
     *
     * @return rebalancer object or null if not engaged.
     */
    Rebalancer getRebalancer() {
        return rebalancer.get();
    }

    Chunk<K, V> creator() {
        return creator.get();
    }

    State state() {
        return state.get();
    }

    private void setState(State state) {
        this.state.set(state);
    }

    void normalize() {
        state.compareAndSet(State.INFANT, State.NORMAL);
        creator.set(null);
        // using fence so other puts can continue working immediately on this chunk
        Chunk.unsafe.storeFence();
    }

    final int getFirstItemEntryIndex() {
        return getEntryField(HEAD_NODE, OFFSET_NEXT);
    }

    private int getLastItemEntryIndex() {
        // find the last sorted entry
        int sortedCount = this.sortedCount.get();
        int entryIndex = sortedCount == 0 ? HEAD_NODE : (sortedCount - 1) * (FIELDS) + 1;
        int nextEntryIndex = getEntryField(entryIndex, OFFSET_NEXT);
        while (nextEntryIndex != Chunk.NONE) {
            entryIndex = nextEntryIndex;
            nextEntryIndex = getEntryField(entryIndex, OFFSET_NEXT);
        }
        return entryIndex;
    }


    public void freeHandle(int handleIndex) {
        handles[handleIndex].remove(memoryManager);
        handles[handleIndex] = null;
    }

    /**
     * freezes chunk so no more changes can be done to it (marks pending items as frozen)
     */
    void freeze() {
        setState(State.FROZEN); // prevent new puts to this chunk
        while (pendingOps.get() != 0) ;
    }

    /***
     * Copies entries from srcChunk performing only entries sorting on the fly
     * (delete entries that are removed as well).
     * @param srcChunk -- chunk to copy from
     * @param srcEntryIdx -- start position for copying
     * @param maxCapacity -- max number of entries "this" chunk can contain after copy
     * @return key index of next to the last copied item, NONE if all items were copied
     */
    final int copyPartNoKeys(Chunk srcChunk, int srcEntryIdx, int maxCapacity) {

        if (srcEntryIdx == HEAD_NODE)
            return NONE;

        // use local variables and just set the atomic variables once at the end
        int sortedEntryIndex = entryIndex.get();
        int currentHandleIdx = handleIndex.get();

        // check that we are not beyond allowed number of entries to copy from source chunk
        int maxIdx = maxCapacity * FIELDS + 1;
        if (sortedEntryIndex >= maxIdx) return srcEntryIdx;
        assert srcEntryIdx <= entries.length - FIELDS;

        // set the next entry index from where we start to copy
        if (sortedEntryIndex != FIRST_ITEM) {
            setEntryField(sortedEntryIndex - FIELDS, OFFSET_NEXT, sortedEntryIndex);
        } else {
            setEntryField(HEAD_NODE, OFFSET_NEXT, FIRST_ITEM);
        }

        int entryIndexStart = srcEntryIdx;
        int entryIndexEnd = entryIndexStart - 1;
        int srcPrevEntryIdx = NONE;
        int currSrcHandleIndex;
        boolean isFirstInInterval = true;

        while (true) {
            currSrcHandleIndex = srcChunk.getEntryField(srcEntryIdx, OFFSET_HANDLE_INDEX);
            int entriesToCopy = entryIndexEnd - entryIndexStart + 1;

            // try to find a continuous interval to copy
            // we cannot enlarge interval: if key is removed (handle index is -1) or
            // if this chunk already has all entries to start with
            if ((currSrcHandleIndex > 0) && (sortedEntryIndex + entriesToCopy * FIELDS <= maxIdx)) {
                // we can enlarge the interval, if it is otherwise possible:
                // if this is first entry in the interval (we need to copy one entry anyway) OR
                // if (on the source chunk) current entry idx directly follows the previous entry idx
                if (isFirstInInterval || (srcPrevEntryIdx + FIELDS == srcEntryIdx)) {
                    entryIndexEnd++;
                    isFirstInInterval = false;
                    srcPrevEntryIdx = srcEntryIdx;
                    srcEntryIdx = srcChunk.getEntryField(srcEntryIdx, OFFSET_NEXT);
                    currSrcHandleIndex = srcChunk.getEntryField(srcEntryIdx, OFFSET_HANDLE_INDEX);
                    if (srcEntryIdx != NONE) continue;

                }
            }

            entriesToCopy = entryIndexEnd - entryIndexStart + 1;
            if (entriesToCopy > 0) { // copy continuous interval (TODO: with arraycopy)
                for (int i = 0; i < entriesToCopy; ++i) {
                    int offset = i * FIELDS;
                    // next should point to the next item
                    entries[sortedEntryIndex + offset + OFFSET_NEXT]
                            = sortedEntryIndex + offset + FIELDS;

                    // copy the values of key position, key block index + key length => 2 integers via array copy
                    System.arraycopy(srcChunk.entries,  // source array
                            entryIndexStart + offset + OFFSET_KEY_POSITION,
                            entries,                        // destination aray
                            sortedEntryIndex + offset + OFFSET_KEY_POSITION, (FIELDS - 2));

                    // copy handle
                    int srcChunkHandleIdx = srcChunk.entries[entryIndexStart + offset + OFFSET_HANDLE_INDEX];
                    assert srcChunk.handles.length > srcChunkHandleIdx;
                    assert srcChunkHandleIdx > 0;
                    assert handles.length > currentHandleIdx;
                    assert currentHandleIdx > 0;
                    entries[sortedEntryIndex + offset + OFFSET_HANDLE_INDEX] = currentHandleIdx;
                    handles[currentHandleIdx] = srcChunk.handles[srcChunkHandleIdx];
                    currentHandleIdx++;
                }

                sortedEntryIndex += entriesToCopy * FIELDS; // update
            }

            if (currSrcHandleIndex < 0) { // if now this is a removed item
                // don't copy it, continue to next item
                srcPrevEntryIdx = srcEntryIdx;
                srcEntryIdx = srcChunk.getEntryField(srcEntryIdx, OFFSET_NEXT);
            }

            if (srcEntryIdx == NONE || sortedEntryIndex > maxIdx)
                break; // if we are done

            // reset and continue
            entryIndexStart = srcEntryIdx;
            entryIndexEnd = entryIndexStart - 1;
            isFirstInInterval = true;

        }

        // next of last item in serial should point to null
        int setIdx = sortedEntryIndex > FIRST_ITEM ? sortedEntryIndex - FIELDS : HEAD_NODE;
        setEntryField(setIdx, OFFSET_NEXT, NONE);
        // update index and counter
        entryIndex.set(sortedEntryIndex);
        handleIndex.set(currentHandleIdx);
        sortedCount.set(sortedEntryIndex / FIELDS);
        statistics.updateInitialSortedCount(sortedCount.get());
        return srcEntryIdx; // if NONE then we finished copying old chunk, else we reached max in new chunk
    }

    /**
     * marks this chunk's next pointer so this chunk is marked as deleted
     *
     * @return the next chunk pointed to once marked (will not change)
     */
    Chunk<K, V> markAndGetNext() {
        // new chunks are ready, we mark frozen chunk's next pointer so it won't change
        // since next pointer can be changed by other split operations we need to do this in a loop - until we succeed
        while (true) {
            // if chunk is marked - that is ok and its next pointer will not be changed anymore
            // return whatever chunk is set as next
            if (next.isMarked()) {
                return next.getReference();
            }
            // otherwise try to mark it
            else {
                // read chunk's current next
                Chunk savedNext = next.getReference();

                // try to mark next while keeping the same next chunk - using CAS
                // if we succeeded then the next pointer we remembered is set and will not change - return it
                if (next.compareAndSet(savedNext, savedNext, false, true))
                    return savedNext;
            }
        }
    }


    boolean shouldRebalance() {
        // perform actual check only in pre defined percentage of puts
        if (ThreadLocalRandom.current().nextInt(100) > REBALANCE_PROB_PERC) return false;

        // if another thread already runs rebalance -- skip it
        if (!isEngaged(null)) return false;
        int numOfEntries = entryIndex.get() / FIELDS;
        int numOfItems = statistics.getCompactedCount();
        int sortedCount = this.sortedCount.get();
        // Reasons for executing a rebalance:
        // 1. There are no sorted keys and the total number of entries is above a certain threshold.
        // 2. There are sorted keys, but the total number of unsorted keys is too big.
        // 3. Out of the occupied entries, there are not enough actual items.
        return (sortedCount == 0 && numOfEntries * MAX_ENTRIES_FACTOR > maxItems) ||
                (sortedCount > 0 && (sortedCount * SORTED_REBALANCE_RATIO) < numOfEntries) ||
                (numOfEntries * MAX_IDLE_ENTRIES_FACTOR > maxItems && numOfItems * MAX_IDLE_ENTRIES_FACTOR < numOfEntries);
    }

    int getNumOfItems() {
        return statistics.getCompactedCount();
    }

    int getNumOfEntries() {
        return entryIndex.get() / FIELDS;
    }

    /*-------------- Iterators --------------*/

    AscendingIter ascendingIter() {
        return new AscendingIter();
    }

    AscendingIter ascendingIter(K from, boolean inclusive) {
        return new AscendingIter(from, inclusive);
    }

    DescendingIter descendingIter() {
        return new DescendingIter();
    }

    DescendingIter descendingIter(K from, boolean inclusive) {
        return new DescendingIter(from, inclusive);
    }

    interface ChunkIter {
        boolean hasNext();

        int next();
    }

    class AscendingIter implements ChunkIter {

        private int next;

        AscendingIter() {
            next = getEntryField(HEAD_NODE, OFFSET_NEXT);
            int handle = getEntryField(next, OFFSET_HANDLE_INDEX);
            while (next != Chunk.NONE && handle < 0) {
                next = getEntryField(next, OFFSET_NEXT);
                handle = getEntryField(next, OFFSET_HANDLE_INDEX);
            }
        }

        AscendingIter(K from, boolean inclusive) {
            next = getEntryField(binaryFind(from), OFFSET_NEXT);

            int handle = getEntryField(next, OFFSET_HANDLE_INDEX);

            int compare = -1;
            if (next != Chunk.NONE)
                compare = compare(from, readKey(next));

            while (next != Chunk.NONE &&
                    (compare > 0 ||
                            (compare >= 0 && !inclusive) ||
                            handle < 0)) {
                next = getEntryField(next, OFFSET_NEXT);
                handle = getEntryField(next, OFFSET_HANDLE_INDEX);
                if (next != Chunk.NONE)
                    compare = compare(from, readKey(next));
            }
        }

        private void advance() {
            next = getEntryField(next, OFFSET_NEXT);
            int handle = getEntryField(next, OFFSET_HANDLE_INDEX);
            while (next != Chunk.NONE && handle < 0) {
                next = getEntryField(next, OFFSET_NEXT);
                handle = getEntryField(next, OFFSET_HANDLE_INDEX);
            }
        }

        @Override
        public boolean hasNext() {
            return next != Chunk.NONE;
        }

        @Override
        public int next() {
            int toReturn = next;
            advance();
            return toReturn;
        }
    }

    class DescendingIter implements ChunkIter {

        private int next;
        private int anchor;
        private int prevAnchor;
        private final IntStack stack;
        private final K from;
        private boolean inclusive;

        DescendingIter() {
            from = null;
            stack = new IntStack(entries.length / FIELDS);
            int sortedCnt = sortedCount.get();
            anchor = sortedCnt == 0 ? HEAD_NODE : (sortedCnt - 1) * (FIELDS) + 1; // this is the last sorted entry
            stack.push(anchor);
            initNext();
        }

        DescendingIter(K from, boolean inclusive) {
            this.from = from;
            this.inclusive = inclusive;
            stack = new IntStack(entries.length / FIELDS);
            anchor = binaryFind(from);
            stack.push(anchor);
            initNext();
        }

        private void initNext() {
            traverseLinkedList();
            advance();
        }

        /**
         * use stack to find a valid next, removed items can't be next
         */
        private void findNewNextInStack() {
            if (stack.empty()) {
                next = Chunk.NONE;
                return;
            }
            next = stack.pop();
            int handle = getEntryField(next, OFFSET_HANDLE_INDEX);
            while (next != Chunk.NONE && handle < 0) {
//            while (next != Chunk.NONE && (handle < 0 || (handle > 0 && handles[handle].isDeleted()))) {
                if (!stack.empty()) {
                    next = stack.pop();
                    handle = getEntryField(next, OFFSET_HANDLE_INDEX);
                } else {
                    next = Chunk.NONE;
                    return;
                }
            }
        }

        /**
         * fill the stack
         */
        private void traverseLinkedList() {
            assert stack.size() == 1; // ancor is in the stack
            if (prevAnchor == getEntryField(anchor, OFFSET_NEXT)) {
                // there is no next;
                next = Chunk.NONE;
                return;
            }
            next = getEntryField(anchor, OFFSET_NEXT);
            if (from == null) {
                while (next != Chunk.NONE) {
                    stack.push(next);
                    next = getEntryField(next, OFFSET_NEXT);
                }
            } else {
                if (inclusive) {
                    while (next != Chunk.NONE && compare(readKey(next), from) <= 0) {
                        stack.push(next);
                        next = getEntryField(next, OFFSET_NEXT);
                    }
                } else {
                    while (next != Chunk.NONE && compare(readKey(next), from) < 0) {
                        stack.push(next);
                        next = getEntryField(next, OFFSET_NEXT);
                    }
                }
            }
        }

        /**
         * find new valid anchor
         */
        private void findNewAnchor() {
            assert stack.empty();
            prevAnchor = anchor;
            if (anchor == HEAD_NODE) {
                next = Chunk.NONE; // there is no more in this chunk
                return;
            } else if (anchor == FIRST_ITEM) {
                anchor = HEAD_NODE;
            } else {
                anchor = anchor - FIELDS;
            }
            stack.push(anchor);
        }

        private void advance() {
            while (true) {
                findNewNextInStack();
                if (next != Chunk.NONE) {
                    return;
                }
                // there is no next in stack
                if (anchor == HEAD_NODE) {
                    // there is no next at all
                    return;
                }
                findNewAnchor();
                traverseLinkedList();
            }
        }

        @Override
        public boolean hasNext() {
            return next != Chunk.NONE;
        }

        @Override
        public int next() {
            int toReturn = next;
            advance();
            return toReturn;
        }

    }

    /**
     * just a simple stack of int, implemented with int array
     */

    class IntStack {

        private final int[] stack;
        private int top;

        IntStack(int size) {
            stack = new int[size];
            top = 0;
        }

        void push(int i) {
            if (top == stack.length) {
                throw new ArrayIndexOutOfBoundsException();
            }
            stack[top] = i;
            top++;
        }

        int pop() {
            if (empty()) {
                throw new EmptyStackException();
            }
            top--;
            return stack[top];
        }

        boolean empty() {
            return top == 0;
        }

        int size() {
            return top;
        }

    }

    /*-------------- Statistics --------------*/

    /**
     * This class contains information about chunk utilization.
     */
    class Statistics {
        private final AtomicInteger addedCount = new AtomicInteger(0);
        private int initialSortedCount = 0;

        /**
         * Initial sorted count here is immutable after chunk re-balance
         */
        void updateInitialSortedCount(int sortedCount) {
            this.initialSortedCount = sortedCount;
        }

        /**
         * @return number of items chunk will contain after compaction.
         */
        int getCompactedCount() {
            return initialSortedCount + getAddedCount();
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

        int getAddedCount() {
            return addedCount.get();
        }

        int getInitialSortedCount() {
            return initialSortedCount;
        }
    }

    /**
     * @return statistics object containing approximate utilization information.
     */
    Statistics getStatistics() {
        return statistics;
    }

}
