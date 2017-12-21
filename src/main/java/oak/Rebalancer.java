package oak;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class Rebalancer {

    /*-------------- Constants --------------*/

    private static final int REBALANCE_SIZE = 2;
    private static final double MAX_AFTER_MERGE_PART = 0.7;
    private static final int LOW_THRESHOLD = Chunk.MAX_ITEMS / 2;
    private static final int MAX_RANGE_TO_APPEND = (int) (0.2 * Chunk.MAX_ITEMS);
    private static final int MAX_AFTER_MERGE_ITEMS = (int) (Chunk.MAX_ITEMS * MAX_AFTER_MERGE_PART);

    /*-------------- Members --------------*/

    private final AtomicReference<Chunk> nextToEngage;
    private final AtomicReference<List<Chunk>> newChunks = new AtomicReference<>(null);
    private final AtomicReference<List<Chunk>> engagedChunks = new AtomicReference<>(null);
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private Chunk first;
    private Chunk last;
    private int chunksInRange;
    private int itemsInRange;
    private final Comparator<ByteBuffer> comparator;
    private final OakMemoryManager memoryManager;
    private final HandleFactory handleFactory;
    private final ValueFactory valueFactory;

    /*-------------- Constructors --------------*/

    Rebalancer(Chunk chunk, Comparator<ByteBuffer> comparator, boolean offHeap, OakMemoryManager memoryManager, HandleFactory handleFactory, ValueFactory valueFactory) {
        this.comparator = comparator;
        this.memoryManager = memoryManager;
        nextToEngage = new AtomicReference<>(chunk);
        this.first = chunk;
        last = chunk;
        chunksInRange = 1;
        itemsInRange = first.getStatistics().getCompactedCount();
        this.handleFactory = handleFactory;
        this.valueFactory = valueFactory;
    }

    static class RebalanceResult {
        boolean success;
        boolean putIfAbsent;

        RebalanceResult(boolean success, boolean putIfAbsent) {
            this.success = success;
            this.putIfAbsent = putIfAbsent;
        }
    }

    /*-------------- Methods --------------*/

    /**
     * compares ByteBuffer by calling the provided comparator
     */
    private int compare(ByteBuffer k1, ByteBuffer k2) {
        return comparator.compare(k1, k2);
    }

    Rebalancer engageChunks() {
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

        List<Chunk> engaged = createEngagedList();

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
    RebalanceResult createNewChunks(ByteBuffer key, ByteBuffer value, Consumer<WritableOakBuffer> function, OakMap.Operation operation) {

        if (this.newChunks.get() != null) {
            return new RebalanceResult(false, false); // this was done by another thread already
        }

        List<Chunk> frozenChunks = engagedChunks.get();

        ListIterator<Chunk> iterFrozen = frozenChunks.listIterator();

        Chunk firstFrozen = iterFrozen.next();
        Chunk currFrozen = firstFrozen;
        Chunk currNewChunk = new Chunk(firstFrozen.minKey, firstFrozen, firstFrozen.comparator, memoryManager);

        int ei = firstFrozen.getFirstItemEntryIndex();

        List<Chunk> newChunks = new LinkedList<>();

        while (true) {
            ei = currNewChunk.copyPart(currFrozen, ei, LOW_THRESHOLD);

            // if completed reading curr frozen chunk
            if (ei == Chunk.NONE) {
                if (!iterFrozen.hasNext())
                    break;

                currFrozen = iterFrozen.next();
                ei = currFrozen.getFirstItemEntryIndex();

            } else { // filled new chunk up to LOW_THRESHOLD

                List<Chunk> frozenSuffix = frozenChunks.subList(iterFrozen.previousIndex(), frozenChunks.size());
                // try to look ahead and add frozen suffix
                if (canAppendSuffix(frozenSuffix, MAX_RANGE_TO_APPEND)) {
                    // maybe there is just a little bit copying left
                    // and we don't want to open a whole new chunk just for it
                    completeCopy(currNewChunk, ei, frozenSuffix);
                    break;
                } else {
                    // we have to open an new chunk
                    // TODO do we want to use slice here?
                    ByteBuffer bb = currFrozen.readKey(ei);
                    int remaining = bb.remaining();
                    int position = bb.position();
                    ByteBuffer newMinKey = ByteBuffer.allocate(remaining);
                    int myPos = newMinKey.position();
                    for (int i = 0; i < remaining; i++) {
                        newMinKey.put(myPos + i, bb.get(i + position));
                    }
                    newMinKey.rewind();
                    Chunk c = new Chunk(newMinKey, firstFrozen, currFrozen.comparator, memoryManager);
                    currNewChunk.next.set(c, false);
                    newChunks.add(currNewChunk);
                    currNewChunk = c;
                }
            }

        }

        newChunks.add(currNewChunk);

        boolean putIfAbsent = true;
        if (operation != OakMap.Operation.NO_OP) { // help this op (for lock freedom)
            putIfAbsent = helpOp(newChunks, key, value, function, operation);
        }

        // if fail here, another thread succeeded, and op is effectively gone
        boolean cas = this.newChunks.compareAndSet(null, newChunks);
        return new RebalanceResult(cas, putIfAbsent);
    }

    private boolean canAppendSuffix(List<Chunk> frozenSuffix, int maxCount) {
        Iterator<Chunk> iter = frozenSuffix.iterator();
        int counter = 0;
        // use statistics to find out how much is left to copy
        while (iter.hasNext() && counter < maxCount) {
            Chunk c = iter.next();
            counter += c.getStatistics().getCompactedCount();
        }
        return counter < maxCount;
    }

    private void completeCopy(Chunk dest, int ei, List<Chunk> srcChunks) {
        Iterator<Chunk> iter = srcChunks.iterator();
        Chunk src = iter.next();
        dest.copyPart(src, ei, Chunk.MAX_ITEMS);
        while (iter.hasNext()) {
            src = iter.next();
            ei = src.getFirstItemEntryIndex();
            dest.copyPart(src, ei, Chunk.MAX_ITEMS);
        }
    }

    private Chunk findNextCandidate() {

        updateRangeView();

        // allow up to RebalanceSize chunks to be engaged
        if (chunksInRange >= REBALANCE_SIZE) return null;

        Chunk candidate = last.next.getReference();

        if (!isCandidate(candidate)) return null;

        int newItems = candidate.getStatistics().getCompactedCount();
        int totalItems = itemsInRange + newItems;
        // TODO think if this makes sense
        int chunksAfterMerge = (int) Math.ceil(((double) totalItems) / MAX_AFTER_MERGE_ITEMS);

        // if the chosen chunk may reduce the number of chunks -- return it as candidate
        if (chunksAfterMerge < chunksInRange + 1) {
            return candidate;
        } else {
            return null;
        }
    }

    private void updateRangeView() {
        while (true) {
            Chunk next = last.next.getReference();
            if (next == null || !next.isEngaged(this)) break;
            last = next;
            addToCounters(last);
        }
    }

    private void addToCounters(Chunk chunk) {
        itemsInRange += chunk.getStatistics().getCompactedCount();
        chunksInRange++;
    }

    /**
     * insert/remove this key and value to one of the newChunks
     * the key is guaranteed to be in the range of keys in the new chunk
     */
    private boolean helpOp(List<Chunk> newChunks, ByteBuffer key, ByteBuffer value, Consumer<WritableOakBuffer> function, OakMap.Operation operation) {
        assert key != null;
        assert operation == OakMap.Operation.REMOVE || value != null;
        assert operation != OakMap.Operation.REMOVE || value == null;

        Chunk c = findChunkInList(newChunks, key);

        // look for key
        Chunk.LookUp lookUp = c.lookUp(key);

        if (lookUp != null && lookUp.handle != null) {
            if(operation == OakMap.Operation.PUT_IF_ABSENT) {
                return false;
            } else if(operation == OakMap.Operation.PUT) {
                lookUp.handle.put(value, memoryManager);
                return true;
            } else if (operation == OakMap.Operation.COMPUTE) {
                lookUp.handle.compute(function, memoryManager);
                return true;
            }
        }
        // TODO handle.put or handle.compute

        int ei;
        if (lookUp == null) { // no entry
            if (operation == OakMap.Operation.REMOVE) return true;
            ei = c.allocateEntryAndKey(key);
            assert ei > 0; // chunk can't be full
            int eiLink = c.linkEntry(ei, key, false);
            assert eiLink == ei; // no one else can insert
        } else {
            ei = lookUp.entryIndex;
        }

        int hi = -1;
        if (operation != OakMap.Operation.REMOVE) {
            hi = c.allocateHandle(handleFactory);
            assert hi > 0; // chunk can't be full


            c.writeValue(hi, valueFactory.createValue(value, memoryManager)); // write value in place

        }

        // set pointer to value
        Chunk.OpData opData = new Chunk.OpData(OakMap.Operation.NO_OP, ei, hi, -1, null);  // prev and op don't matter
        c.pointToValueCAS(opData, false);

        return true;
    }


    /**
     * @return a chunk from the list that can hold the given key
     */
    private Chunk findChunkInList(List<Chunk> newChunks, ByteBuffer key) {
        Iterator<Chunk> iter = newChunks.iterator();
        assert iter.hasNext();
        Chunk next = iter.next();
        Chunk prev = next;
        assert compare(prev.minKey, key) <= 0;

        while (iter.hasNext()) {
            prev = next;
            next = iter.next();
            if (compare(next.minKey, key) > 0) {
                // if we went to far
                break;
            } else {
                // check next chunk
                prev = next; // maybe there won't be any next, so set this here
            }
        }

        return prev;
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

    private List<Chunk> createEngagedList() {
        Chunk current = first;
        List<Chunk> engaged = new LinkedList<>();

        while (current != null && current.isEngaged(this)) {
            engaged.add(current);
            current = current.next.getReference();
        }

        if (engaged.isEmpty()) throw new IllegalStateException("Engaged list cannot be empty");

        return engaged;
    }

    List<Chunk> getEngagedChunks() {
        List<Chunk> engaged = engagedChunks.get();
        if (engaged == null) throw new IllegalStateException("Trying to get engaged before engagement stage completed");
        return engaged;
    }

    List<Chunk> getNewChunks() {
        List<Chunk> newChunks = this.newChunks.get();
        if (newChunks == null)
            throw new IllegalStateException("Trying to get new chunks before creating stage completed");
        return newChunks;
    }

}
