package oak;

import javafx.util.Pair;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.EmptyStackException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

public class Chunk {

    /*-------------- Constants --------------*/

    static final int NONE = 0;    // constant for "no index", etc. MUST BE 0!

    // location of the first (head) node - just a next pointer
    private static final int HEAD_NODE = 0;
    // index of first item in array, after head (not necessarily first in list!)
    private static final int FIRST_ITEM = 1;

    private static final int FIELDS = 4;  // # of fields in each item of key array
    private static final int OFFSET_NEXT = 0;
    private static final int OFFSET_KEY_INDEX = 1;
    private static final int OFFSET_KEY_LENGTH = 2;
    private static final int OFFSET_HANDLE_INDEX = 3;

    static int MAX_ITEMS = 2048;
    static int MAX_THREADS = 32;

    // used for checking if rebalance is needed
    private static final double REBALANCE_PROB_PERC = 15;
    private static final double SORTED_REBALANCE_RATIO = 1.6;

    // when chunk is frozen, all of the elements in pending puts array will be this OpData
    private static final OpData FROZEN_OP_DATA = new OpData(OakMap.Operation.NO_OP, 0, 0, 0, null);

    /*-------------- Members --------------*/

    private static final Unsafe unsafe;
    private final OakMemoryManager memoryManager;
    ByteBuffer minKey;       // minimal key that can be put in this chunk
    AtomicMarkableReference<Chunk> next;
    Comparator<ByteBuffer> comparator;
    private ByteBuffer[] byteBufferPerThread;
    private AtomicReference<Chunk> creator;     // in split/compact process, represents parent of split (can be null!)
    private final AtomicReference<State> state; // chunk can be in the following states: normal, frozen or infant(has a creator)
    private AtomicReference<Rebalancer> rebalancer;
    private final int[] entries;    // array is initialized to 0, i.e., NONE - this is important!
    private final KeysManager keysManager;
    private final Handle[] handles;
    private AtomicReferenceArray<OpData> pendingOps;
    private final AtomicInteger entryIndex;    // points to next free index of entry array
    private final AtomicInteger keyIndex;    // points to next free index of key array
    private final AtomicInteger handleIndex;    // points to next free index of entry array
    private final Statistics statistics;
    private int sortedCount;      // # of sorted items at entry-array's beginning (resulting from split)

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
     * @param minKey  minimal key to be placed in chunk
     * @param creator the chunk that is responsible for this chunk creation
     */
    Chunk(ByteBuffer minKey, Chunk creator, Comparator<ByteBuffer> comparator, OakMemoryManager memoryManager) {
        this.memoryManager = memoryManager;
        this.entries = new int[MAX_ITEMS * FIELDS + FIRST_ITEM];
        this.entryIndex = new AtomicInteger(FIRST_ITEM);
        this.handles = new Handle[MAX_ITEMS + FIRST_ITEM];
        this.handleIndex = new AtomicInteger(FIRST_ITEM);
        if (memoryManager != null) {
            this.keysManager = new KeysManagerOffHeapImpl(MAX_ITEMS * 100, memoryManager);  // TODO how big should this be?
        } else {
            this.keysManager = new KeysManagerOnHeapImpl(MAX_ITEMS * 100);
        }
        this.keyIndex = new AtomicInteger(FIRST_ITEM);
        this.sortedCount = 0;
        this.minKey = minKey; // TODO copy given key
        this.creator = new AtomicReference<>(creator);
        if (creator == null)
            this.state = new AtomicReference<>(State.NORMAL);
        else
            this.state = new AtomicReference<>(State.INFANT);
        this.next = new AtomicMarkableReference<>(null, false);
        this.pendingOps = new AtomicReferenceArray<>(MAX_THREADS);
        this.rebalancer = new AtomicReference<>(null); // to be updated on rebalance
        this.statistics = new Statistics();
        this.comparator = comparator;
        this.byteBufferPerThread = new ByteBuffer[MAX_THREADS]; // init to null
    }

    enum State {
        INFANT,
        NORMAL,
        FROZEN,
        RELEASED
    }

    static class OpData {
        OakMap.Operation op;
        int entryIndex;
        int handleIndex;
        int prevHandleIndex;
        Consumer<WritableOakBuffer> function;

        OpData(OakMap.Operation op, int entryIndex, int handleIndex, int prevHandleIndex, Consumer<WritableOakBuffer> function) {
            this.op = op;
            this.entryIndex = entryIndex;
            this.handleIndex = handleIndex;
            this.prevHandleIndex = prevHandleIndex;
            this.function = function;
        }
    }

    /*-------------- Methods --------------*/

    static int getIndex() {
        // TODO use hash instead of modulo
        return (int) (Thread.currentThread().getId() % MAX_THREADS);
    }

    void release() {
        if (state.compareAndSet(State.FROZEN, State.RELEASED)) {
            keysManager.release();
        }
    }

    /**
     * compares ByteBuffer by calling the provided comparator
     */
    private int compare(ByteBuffer k1, ByteBuffer k2) {
        return comparator.compare(k1, k2);
    }

    /**
     * performs CAS from 'expected' to 'value' for field at specified offset of given item in key array
     */
    private boolean casEntryArray(int item, int offset, int expected, int value) {
        return unsafe.compareAndSwapInt(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expected, value);
    }

    /**
     * performs CAS from 'expected' to 'value' for field at specified offset of given item in pending array
     */
    private boolean casPendingArray(int item, OpData expected, OpData opData) {
        return pendingOps.compareAndSet(item, expected, opData);
    }

    ByteBuffer readKey(int entryIndex) {
        if (entryIndex == Chunk.NONE) {
            return null;
        }
        int ki = get(entryIndex, OFFSET_KEY_INDEX);
        int length = get(entryIndex, OFFSET_KEY_LENGTH);

        int idx = getIndex();
        if (byteBufferPerThread[idx] == null) {
            byteBufferPerThread[idx] = keysManager.getKeys().asReadOnlyBuffer();
        }
        ByteBuffer bbThread = byteBufferPerThread[idx];
        int pos = keysManager.getPosition();
        bbThread.limit(pos + ki + length);
        bbThread.position(pos + ki);
        return bbThread;
    }

    /**
     * gets the field of specified offset for given item in entry array
     */
    private int get(int item, int offset) {
        return entries[item + offset];
    }

    /**
     * sets the field of specified offset to 'value' for given item in entry array
     */
    private void set(int item, int offset, int value) {
        assert item + offset >= 0;
        entries[item + offset] = value;
    }

    /**
     * write key in place
     **/
    private void writeKey(ByteBuffer key, int ki, int length) {
        keysManager.writeKey(key, ki, length);
    }

    /**
     * gets the value for the given item, or 'null' if it doesn't exist
     */
    Handle getHandle(int entryIndex) {

        int hi = get(entryIndex, OFFSET_HANDLE_INDEX);

        // if no value for item - return null
        assert hi != 0; // because we got to it from linked list
        if (hi < 0) {
            return null;
        } else {
            return handles[hi];
        }
    }

    /**
     * look up key
     */
    LookUp lookUp(ByteBuffer key) {
        // binary search sorted part of key array to quickly find node to start search at
        // it finds previous-to-key so start with its next
        int curr = get(binaryFind(key), OFFSET_NEXT);
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
                int hi = get(curr, OFFSET_HANDLE_INDEX);
                if (hi < 0) return new LookUp(null, curr, -1);
                Handle h = handles[hi];
                if (h.isDeleted()) return new LookUp(null, curr, hi);
                return new LookUp(h, curr, hi);
            }
            // otherwise- proceed to next item
            else
                curr = get(curr, OFFSET_NEXT);
        }
        return null;
    }

    class LookUp {

        Handle handle;
        int entryIndex;
        int handleIndex;

        LookUp(Handle handle, int entryIndex, int handleIndex) {
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
    private int binaryFind(ByteBuffer key) {
        // if there are no sorted keys, or the first item is already larger than key -
        // return the head node for a regular linear search
        if ((sortedCount == 0) || compare(readKey(FIRST_ITEM), key) >= 0)
            return HEAD_NODE;

        // TODO locality in binary search? check last key to avoid binary search? these are optimizations we can try

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
    boolean publish(OpData opData) {

        int idx = getIndex();
        // TODO verify the assumption about sequential IDs, for now leave it because PPA will change (no MAX_THREADS)

        // publish into thread array
        return casPendingArray(idx, null, opData);
    }

    /**
     * unpublish operation from thread array
     * if CAS didn't succeed then this means that a rebalancer did this already
     **/
    void unpublish(OpData oldOpData) {
        int idx = getIndex();
        casPendingArray(idx, oldOpData, null); // publish into thread array
    }

    /**
     * allocate value handle
     *
     * @return if chunk is full return -1, otherwise return new handle index
     **/
    int allocateHandle(HandleFactory handleFactory) {
        int hi = handleIndex.getAndIncrement();
        if (hi + 1 > handles.length) {
            return -1;
        }
        handles[hi] = handleFactory.createHandle();
        return hi;
    }

    int allocateEntryAndKey(ByteBuffer key) {
        int ei = entryIndex.getAndAdd(FIELDS);
        if (ei + FIELDS > entries.length) {
            return -1;
        }

        int length = key.remaining(); // TODO is this the length?
        int ki = keyIndex.getAndAdd(length);
        if (ki + length >= keysManager.length()) { // TODO is this the right check?
            return -1;
        }

        // key and value must be set before linking to the list so it will make sense when reached before put is done
        set(ei, OFFSET_HANDLE_INDEX, -1); // set value index to -1, value is init to null
        writeKey(key, ki, length);
        set(ei, OFFSET_KEY_INDEX, ki);
        set(ei, OFFSET_KEY_LENGTH, length);

        return ei;
    }

    int linkEntry(int ei, ByteBuffer key, boolean cas) {
        int prev, curr, cmp;
        int anchor = -1;

        while (true) {
            // start iterating from quickly-found node (by binary search) in sorted part of order-array
            if (anchor == -1) anchor = binaryFind(key);
            curr = anchor;

            // iterate items until key's position is found
            while (true) {
                prev = curr;
                curr = get(prev, OFFSET_NEXT);    // index of next item in list

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
            set(ei, OFFSET_NEXT, curr); // no need for CAS since put is not even published yet
            if (cas) {
                if (casEntryArray(prev, OFFSET_NEXT, curr, ei)) {
                    return ei;
                }
                // CAS didn't succeed, try again
            } else {
                // without CAS (used by rebalance)
                set(prev, OFFSET_NEXT, ei);
            }
        }
    }

    /**
     * write value in place
     **/
    void writeValue(int hi, Pair<Integer, ByteBuffer> pair) {
        assert memoryManager != null || pair.getKey() == 0;
        handles[hi].setValue(pair.getValue(), pair.getKey());
    }

    /**
     * point to value
     * if unsuccessful this means someone else got to it first (helping rebalancer or other operation)
     */
    boolean pointToValue(OpData opData) {

        if (pointToValueCAS(opData, true)) {
            return true;
        }

        OakMap.Operation operation = opData.op;

        if (operation == OakMap.Operation.REMOVE) {
            return true; // this is a remove, no need to try again and return doesn't matter
        }

        int handle = opData.handleIndex;
        int now = get(opData.entryIndex, OFFSET_HANDLE_INDEX);

        if (now == handle) {
            return true; // someone helped
        } else if (now < 0) {
            opData.prevHandleIndex = -1;
            return pointToValue(opData); // remove completed, try again
        } else if (operation == OakMap.Operation.PUT_IF_ABSENT) {
            return false; // to late
        } else if (operation == OakMap.Operation.COMPUTE){
            Handle h = handles[now];
            if(h != null){
                h.compute(opData.function,memoryManager);
            }
            return true;
        }
        // this is a put, try again
        opData.prevHandleIndex = now;
        return pointToValue(opData);
    }

    /**
     * used by put/putIfAbsent/remove and rebalancer
     */
    boolean pointToValueCAS(OpData opData, boolean cas) {
        if (cas) {
            if (casEntryArray(opData.entryIndex, OFFSET_HANDLE_INDEX, opData.prevHandleIndex, opData.handleIndex)) {
                // update statistics only by thread that CASed
                if (opData.prevHandleIndex < 0 && opData.handleIndex > 0) { // previously a remove
                    statistics.incrementAddedCount();
                } else if (opData.prevHandleIndex > 0 && opData.handleIndex == -1) { // removing
                    statistics.decrementAddedCount();
                }
                return true;
            }
        } else {
            set(opData.entryIndex, OFFSET_HANDLE_INDEX, opData.handleIndex);
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

    Chunk creator() {
        return creator.get();
    }

    State state() {
        return state.get();
    }

    private void setState(State state) {
        this.state.set(state);
    }

    void normalize() {
        setState(State.NORMAL);
        creator.set(null);
    }

    final int getFirstItemEntryIndex() {
        return get(HEAD_NODE, OFFSET_NEXT);
    }

    /**
     * freezes chunk so no more changes can be done to it (marks pending items as frozen)
     */
    void freeze() {
        setState(State.FROZEN); // prevent new puts to this chunk

        // go over pending of all threads
        for (int i = 0; i < MAX_THREADS; ++i) {
            OpData opData = pendingOps.get(i);
            if (opData == FROZEN_OP_DATA) {
                // frozen already
                continue;
            }
            boolean casSuccess;
            if (opData == null) {
                casSuccess = casPendingArray(i, null, FROZEN_OP_DATA);
                // can fail if there is a new pending op or some other thread froze it already
            } else { // need to help this op
                pointToValue(opData); // this is the helping part
                casSuccess = casPendingArray(i, opData, FROZEN_OP_DATA);
                // can fail if unpublished or some other thread froze it already
            }

            if (!casSuccess) { // try again
                opData = pendingOps.get(i);
                if (opData == FROZEN_OP_DATA) {
                    // frozen already
                    continue;
                }
                if (opData == null) {
                    casSuccess = casPendingArray(i, null, FROZEN_OP_DATA);
                } else { // need to help this put
                    pointToValue(opData); // this is the helping part
                    casPendingArray(i, opData, null);
                    // a thread can unpublish here
                    // so we make sure we this will freeze even if a thread did unpublish here
                    casSuccess = casPendingArray(i, null, FROZEN_OP_DATA);
                }
                // the frozen was set beforehand
                // therefor a put could not publish or unpublish again
                opData = pendingOps.get(i);
                assert (casSuccess || opData == FROZEN_OP_DATA);
            }

        }
    }

    /***
     * Copies items from srcChunk performing compaction on the fly.
     * @param srcChunk -- chunk to copy from
     * @param ei -- start position for copying
     * @param maxCapacity -- max number of items "this" chunk can contain after copy
     * @return key index of next to the last copied item, NONE if all items were copied
     */
    final int copyPart(Chunk srcChunk, int ei, int maxCapacity) {

        assert ei != HEAD_NODE;

        // use local variables and just set the atomic variables once at the end
        int sortedEntryIndex = entryIndex.get();
        int sortedKeyIndex = keyIndex.get();
        int sortedHandleIndex = handleIndex.get();


        int maxIdx = maxCapacity * FIELDS + 1;
        if (sortedEntryIndex >= maxIdx) return ei;
        assert ei < entries.length - FIELDS;
        // TODO check if reached capacity with key index?
        if (sortedEntryIndex != FIRST_ITEM) {
            set(sortedEntryIndex - FIELDS, OFFSET_NEXT, sortedEntryIndex);
        } else {
            set(HEAD_NODE, OFFSET_NEXT, FIRST_ITEM);
        }

        int sortedSize = srcChunk.sortedCount * FIELDS + 1;
        int entryIndexStart = ei;
        int entryIndexEnd = entryIndexStart - 1;
        int eiPrev = NONE;
        int currHandleIndex;
        int currKeyIndex;
        int prevKeyIndex = NONE;
        int prevKeyLength = 0;
        int keyLengthToCopy = 0;
        boolean isFirst = true;

        while (true) {
            currHandleIndex = srcChunk.get(ei, OFFSET_HANDLE_INDEX);
            currKeyIndex = srcChunk.get(ei, OFFSET_KEY_INDEX);
            int itemsToCopy = entryIndexEnd - entryIndexStart + 1;

            // try to find a continuous interval to copy
            // first check that this key is not a removed key
            // save this item if this will be the first in the continuous interval
            // or save this item if it create a continuous interval with the previously saved item
            // which means it's key index is adjacent to prev's key index
            // and there is still room
            if ((currHandleIndex > 0) && (isFirst || (eiPrev < sortedSize)
                    &&
                    (eiPrev + FIELDS == ei)
                    &&
                    (sortedEntryIndex + itemsToCopy * FIELDS <= maxIdx)
                    &&
                    (prevKeyIndex + prevKeyLength == currKeyIndex))) { // TODO we should add this check right?

                entryIndexEnd++;
                isFirst = false;
                eiPrev = ei;
                ei = srcChunk.get(ei, OFFSET_NEXT);
                prevKeyIndex = currKeyIndex;
                prevKeyLength = srcChunk.get(eiPrev, OFFSET_KEY_LENGTH);
                keyLengthToCopy += prevKeyLength;
                if (ei != NONE) continue;
            }

            itemsToCopy = entryIndexEnd - entryIndexStart + 1;
            if (itemsToCopy > 0) { // copy continuous interval (with arraycopy)
                // first copy key array
                int sortedKI = sortedKeyIndex;
                for (int i = 0; i < itemsToCopy; ++i) {
                    int offset = i * FIELDS;
                    // next should point to the next item
                    entries[sortedEntryIndex + offset + OFFSET_NEXT] = sortedEntryIndex + offset + FIELDS;
                    entries[sortedEntryIndex + offset + OFFSET_KEY_INDEX] = sortedKI;
                    int keyLength = srcChunk.entries[entryIndexStart + offset + OFFSET_KEY_LENGTH];
                    entries[sortedEntryIndex + offset + OFFSET_KEY_LENGTH] = keyLength;
                    sortedKI += keyLength;
                    // copy handle
                    handles[sortedHandleIndex] = srcChunk.handles[srcChunk.entries[entryIndexStart + offset + OFFSET_HANDLE_INDEX]];
                    entries[sortedEntryIndex + offset + OFFSET_HANDLE_INDEX] = sortedHandleIndex;
                    sortedHandleIndex++;
                }
                sortedEntryIndex += itemsToCopy * FIELDS; // update

                // now copy key array
                int keyIdx = srcChunk.get(entryIndexStart, OFFSET_KEY_INDEX);
                keysManager.copyKeys(srcChunk.keysManager, keyIdx, sortedKeyIndex, keyLengthToCopy);

                sortedKeyIndex += keyLengthToCopy; // update
            }

            if (currHandleIndex < 0) { // if now this is a removed item
                // don't copy it, continue to next item
                eiPrev = ei;
                ei = srcChunk.get(ei, OFFSET_NEXT);
            }

            if (ei == NONE || sortedEntryIndex > maxIdx) // TODO check if reached capacity with key index?
                break; // if we are done

            // reset and continue
            entryIndexStart = ei;
            entryIndexEnd = entryIndexStart - 1;
            keyLengthToCopy = 0;
            isFirst = true;

        }

        // next of last item in serial should point to null
        int setIdx = sortedEntryIndex > FIRST_ITEM ? sortedEntryIndex - FIELDS : HEAD_NODE;
        set(setIdx, OFFSET_NEXT, NONE);
        // update index and counter
        entryIndex.set(sortedEntryIndex);
        keyIndex.set(sortedKeyIndex);
        handleIndex.set(sortedHandleIndex);
        sortedCount = sortedEntryIndex / FIELDS;

        return ei; // if NONE then we finished copying old chunk, else we reached max in new chunk
    }

    /**
     * marks this chunk's next pointer so this chunk is marked as deleted
     *
     * @return the next chunk pointed to once marked (will not change)
     */
    Chunk markAndGetNext() {
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
        // TODO change after adding support to variable size keys?
        // if another thread already runs rebalance -- skip it
        if (!isEngaged(null)) return false;
        int numOfEntries = entryIndex.get() / FIELDS;
        int numOfItems = statistics.getCompactedCount();
        return (sortedCount == 0 && numOfEntries << 3 > Chunk.MAX_ITEMS) ||
                (sortedCount > 0 && (sortedCount * SORTED_REBALANCE_RATIO) < numOfEntries) ||
                (numOfEntries << 3 > Chunk.MAX_ITEMS && numOfItems << 2 < numOfEntries);
    }

    /*-------------- Iterators --------------*/

    AscendingIter ascendingIter() {
        return new AscendingIter();
    }

    AscendingIter ascendingIter(ByteBuffer from) {
        return new AscendingIter(from);
    }

    DescendingIter descendingIter() {
        return new DescendingIter();
    }

    DescendingIter descendingIter(ByteBuffer from, boolean inclusive) {
        return new DescendingIter(from, inclusive);
    }

    interface ChunkIter {
        boolean hasNext();

        int next();
    }

    class AscendingIter implements ChunkIter {

        private int next;

        AscendingIter() {
            next = get(HEAD_NODE, OFFSET_NEXT);
            int handle = get(next, OFFSET_HANDLE_INDEX);
            while (next != Chunk.NONE && handle < 0) {
//                while (next != oak.Chunk.NONE && (handle < 0 || (handle > 0 && handles[handle].isDeleted()))) {
                // if there is a next but it was removed then try next item
                next = get(next, OFFSET_NEXT);
                handle = get(next, OFFSET_HANDLE_INDEX);
            }
        }

        AscendingIter(ByteBuffer from) {
            next = get(binaryFind(from), OFFSET_NEXT);
            int handle = get(next, OFFSET_HANDLE_INDEX);
            while (next != Chunk.NONE && handle < 0) {
//                while (next != oak.Chunk.NONE && (handle < 0 || (handle > 0 && handles[handle].isDeleted()))) {
                // if there is a next but it was removed then try next item
                next = get(next, OFFSET_NEXT);
                handle = get(next, OFFSET_HANDLE_INDEX);
            }
        }

        private void advance() {
            next = get(next, OFFSET_NEXT);
            int handle = get(next, OFFSET_HANDLE_INDEX);
            while (next != Chunk.NONE && handle < 0) {
//                while (next != oak.Chunk.NONE && (handle < 0 || (handle > 0 && handles[handle].isDeleted()))) {
                // if there is a next but it was removed then try next item
                next = get(next, OFFSET_NEXT);
                handle = get(next, OFFSET_HANDLE_INDEX);
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
        private IntStack stack;
        private ByteBuffer from;
        private boolean inclusive;

        DescendingIter() {
            from = null;
            stack = new IntStack(entries.length / FIELDS); // TODO maybe optimization is needed here
            anchor = sortedCount == 0 ? HEAD_NODE : (sortedCount - 1) * (FIELDS) + 1; // this is the last sorted entry
            stack.push(anchor);
            initNext();
        }

        DescendingIter(ByteBuffer from, boolean inclusive) {
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
            int handle = get(next, OFFSET_HANDLE_INDEX);
            while (next != Chunk.NONE && handle < 0) {
//            while (next != oak.Chunk.NONE && (handle < 0 || (handle > 0 && handles[handle].isDeleted()))) {
                if (!stack.empty()) {
                    next = stack.pop();
                    handle = get(next, OFFSET_HANDLE_INDEX);
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
            if (prevAnchor == get(anchor, OFFSET_NEXT)) {
                // there is no next;
                next = Chunk.NONE;
                return;
            }
            next = get(anchor, OFFSET_NEXT);
            if (from == null) {
                while (next != Chunk.NONE) {
                    stack.push(next);
                    next = get(next, OFFSET_NEXT);
                }
            } else {
                if (inclusive) {
                    while (next != Chunk.NONE && compare(readKey(next), from) <= 0) {
                        stack.push(next);
                        next = get(next, OFFSET_NEXT);
                    }
                } else {
                    while (next != Chunk.NONE && compare(readKey(next), from) < 0) {
                        stack.push(next);
                        next = get(next, OFFSET_NEXT);
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

        private int stack[];
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
    protected class Statistics {
        private AtomicInteger addedCount = new AtomicInteger(0);

        /**
         * @return number of items chunk will contain after compaction.
         */
        int getCompactedCount() {
            return sortedCount + getAddedCount();
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
    }

    /**
     * @return statistics object containing approximate utilization information.
     */
    Statistics getStatistics() {
        return statistics;
    }

}
