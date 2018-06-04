package oak;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class OakMapOffHeapImpl implements OakMap, AutoCloseable {

    /*-------------- Members --------------*/

    Logger log = Logger.getLogger(OakMapOffHeapImpl.class.getName());
    final ConcurrentSkipListMap<Object, Chunk> skiplist;    // skiplist of chunks for fast navigation
    private final AtomicReference<Chunk> head;
    private final ByteBuffer minKey;
    private final Comparator comparator;
    final OakMemoryManager memoryManager;
    private final HandleFactory handleFactory;
    private final ValueFactory valueFactory;
    private AtomicInteger size;

    /**
     * Lazily initialized descending key set
     */
    private OakMap descendingMap;

    /*-------------- Constructors --------------*/

    /**
     * init with capacity = 2g
     */
    public OakMapOffHeapImpl(int chunkMaxItems, int chunkBytesPerItem) {
        this(StaticPoolFactory.getPool(), chunkMaxItems, chunkBytesPerItem);
    }

    public OakMapOffHeapImpl(MemoryPool memoryPool, int chunkMaxItems, int chunkBytesPerItem) {
        ByteBuffer bb = ByteBuffer.allocate(1).put(Byte.MIN_VALUE);
        bb.rewind();
        this.minKey = bb;
        this.size = new AtomicInteger(0);

        this.comparator = Comparator.naturalOrder();

        this.memoryManager = new OakMemoryManager(memoryPool);

        this.skiplist = new ConcurrentSkipListMap<>();
        Chunk head = new Chunk(this.minKey, null, comparator, memoryManager, chunkMaxItems, chunkBytesPerItem, this.size);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);

        this.descendingMap = null;
        this.handleFactory = new HandleFactory(true);
        this.valueFactory = new ValueFactory(true);
    }

    /**
     * init with capacity = 2g
     */
    public OakMapOffHeapImpl(Comparator<Object> comparator,
                             ByteBuffer minKey,
                             int chunkMaxItems,
                             int chunkBytesPerItem) {
        this(comparator, minKey, StaticPoolFactory.getPool(), chunkMaxItems, chunkBytesPerItem);
    }

    public OakMapOffHeapImpl(Comparator<Object> comparator,
                             Object minKey,
                             Consumer<KeyInfo> keyCreator,
                             Function<Object, Integer> keyCapacityCalculator,
                             int chunkMaxItems,
                             int chunkBytesPerItem) {
        this(comparator, minKey, keyCreator, keyCapacityCalculator, StaticPoolFactory.getPool(), chunkMaxItems, chunkBytesPerItem);
    }

    public OakMapOffHeapImpl(Comparator<Object> comparator,
                             ByteBuffer minKey,
                             MemoryPool memoryPool,
                             int chunkMaxItems,
                             int chunkBytesPerItem) {
        ByteBuffer bb = ByteBuffer.allocate(minKey.remaining());
        for (int i = 0; i < minKey.limit(); i++) {
            bb.put(i, minKey.get(i));
        }
        this.minKey = bb;
        this.size = new AtomicInteger(0);
        this.comparator = comparator;

        this.memoryManager = new OakMemoryManager(memoryPool);

        this.skiplist = new ConcurrentSkipListMap<>(comparator);
        Chunk head = new Chunk(this.minKey, null, comparator, memoryManager, chunkMaxItems, chunkBytesPerItem, this.size);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);

        this.descendingMap = null;
        this.handleFactory = new HandleFactory(true);
        this.valueFactory = new ValueFactory(true);
    }

    public OakMapOffHeapImpl(Comparator<Object> comparator,
                             Object minKey,
                             Consumer<KeyInfo> keyCreator,
                             Function<Object, Integer> keyCapacityCalculator,
                             MemoryPool memoryPool,
                             int chunkMaxItems,
                             int chunkBytesPerItem) {
        this.minKey = ByteBuffer.allocate(keyCapacityCalculator.apply(minKey));
        this.minKey.position(0);
        KeyInfo minKeyInfo = new KeyInfo(this.minKey, 0, minKey);
        keyCreator.accept(minKeyInfo);
        this.size = new AtomicInteger(0);
        this.comparator = comparator;

        this.memoryManager = new OakMemoryManager(memoryPool);

        this.skiplist = new ConcurrentSkipListMap<>(comparator);
        Chunk head = new Chunk(this.minKey, null, comparator, memoryManager, chunkMaxItems, chunkBytesPerItem, this.size);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);

        this.descendingMap = null;
        this.handleFactory = new HandleFactory(true);
        this.valueFactory = new ValueFactory(true);
    }

    /*-------------- Closable --------------*/

    /**
     * cleans off heap memory
     */
    @Override
    public void close() {
        memoryManager.pool.clean();
    }

    /*-------------- size --------------*/

    /**
     * @return current off heap memory usage in bytes
     */
    public long size() {
        return memoryManager.pool.allocated();
    }

    public int entries() { return size.get(); }

    /*-------------- Methods --------------*/

    /**
     * finds and returns the chunk where key should be located, starting from given chunk
     */
    private Chunk iterateChunks(Chunk c, Object key) {
        // find chunk following given chunk (next)
        Chunk next = c.next.getReference();

        // since skiplist isn't updated atomically in split/compaction, our key might belong in the next chunk
        // we need to iterate the chunks until we find the correct one
        while ((next != null) && (comparator.compare(next.minKey, key) <= 0)) {
            c = next;
            next = c.next.getReference();
        }

        return c;
    }

    private Rebalancer.RebalanceResult rebalance(Chunk c, Object opKey, ByteBuffer opValue, Consumer<WritableOakBuffer> function, Operation op) {
        if (c == null) {
            assert op == Operation.NO_OP;
            return null;
        }
        Rebalancer rebalancer = new Rebalancer(c, comparator, true, memoryManager, handleFactory, valueFactory);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        Rebalancer.RebalanceResult result = rebalancer.createNewChunks(opKey, opValue, function, op); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk> newChunks = rebalancer.getNewChunks();
        List<Chunk> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        for (Chunk chunk : engaged
                ) {
            chunk.release();
        }

        return result;
    }

    private Rebalancer.RebalanceResult rebalance(Chunk c,
                                                 Object opKey,
                                                 Consumer<KeyInfo> opKeyCreator,
                                                 Function<Object, Integer> opKeyCapacityCalculator,
                                                 Consumer<ByteBuffer> opValueCreator,
                                                 int opValueCapacity,
                                                 Consumer<WritableOakBuffer> function,
                                                 Operation op) {
        if (c == null) {
            assert op == Operation.NO_OP;
            return null;
        }
        Rebalancer rebalancer = new Rebalancer(c, comparator, true, memoryManager, handleFactory, valueFactory);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        Rebalancer.RebalanceResult result = rebalancer.createNewChunks(opKey, opKeyCreator, opKeyCapacityCalculator, opValueCreator, opValueCapacity, function, op); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk> newChunks = rebalancer.getNewChunks();
        List<Chunk> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        for (Chunk chunk : engaged
                ) {
            chunk.release();
        }

        if (result.success && result.putIfAbsent) {
            size.incrementAndGet();
        }

        return result;
    }

    private void checkRebalance(Chunk c) {
        if (c.shouldRebalance()) {
            rebalance(c, null, null, null, null, 0, null, Operation.NO_OP);
        }
    }

    private void connectToChunkList(List<Chunk> engaged, List<Chunk> children) {

        updateLastChild(engaged, children);

        Chunk firstEngaged = engaged.get(0);

        // replace in linked list - we now need to find previous chunk to our chunk
        // and CAS its next to point to c1, which is the same c1 for all threads who reached this point
        // since prev might be marked (in compact itself) - we need to repeat this until successful
        while (true) {
            // start with first chunk (i.e., head)
            Entry<Object, Chunk> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

            Chunk prev = lowerEntry != null ? lowerEntry.getValue() : null;
            Chunk curr = (prev != null) ? prev.next.getReference() : null;

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
            if ((curr == null) || (prev == null)) {
                break;
            }

            // if prev chunk is marked - it is deleted, need to help split it and then continue
            if (prev.next.isMarked()) {
                rebalance(prev, null, null, null, Operation.NO_OP);
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

    private void updateLastChild(List<Chunk> engaged, List<Chunk> children) {
        Chunk lastEngaged = engaged.get(engaged.size() - 1);
        Chunk nextToLast = lastEngaged.markAndGetNext(); // also marks last engaged chunk as deleted
        Chunk lastChild = children.get(children.size() - 1);

        lastChild.next.compareAndSet(null, nextToLast, false, false);
    }

    private void updateIndexAndNormalize(List<Chunk> engagedChunks, List<Chunk> children) {
        Iterator<Chunk> iterEngaged = engagedChunks.iterator();
        Iterator<Chunk> iterChildren = children.iterator();

        Chunk firstEngaged = iterEngaged.next();
        Chunk firstChild = iterChildren.next();

        // need to make the new chunks available, before removing old chunks
        skiplist.replace(firstEngaged.minKey, firstEngaged, firstChild);

        // remove all old chunks from index.
        while (iterEngaged.hasNext()) {
            Chunk engagedToRemove = iterEngaged.next();
            skiplist.remove(engagedToRemove.minKey, engagedToRemove); // conditional remove is used
        }

        // now after removing old chunks we can start normalizing
        firstChild.normalize();
        // TODO do we still need this? oak.Chunk.unsafe.storeFence();
        // using fence so puts can continue working immediately on this chunk

        // for simplicity -  naive lock implementation
        // can be implemented without locks using versions on next pointer in skiplist
        while (iterChildren.hasNext()) {
            Chunk childToAdd;
            synchronized (childToAdd = iterChildren.next()) {
                if (childToAdd.state() == Chunk.State.INFANT) { // make sure it wasn't add before
                    skiplist.putIfAbsent(childToAdd.minKey, childToAdd);
                    childToAdd.normalize();
                }
                // has a built in fence, so no need to add one here
            }
        }
    }

    private boolean rebalancePutIfAbsent(Chunk c, ByteBuffer key, ByteBuffer value) {
        Rebalancer.RebalanceResult result = rebalance(c, key, value, null, Operation.PUT_IF_ABSENT);
        assert result != null;
        if (!result.success) { // rebalance helped put
            return putIfAbsent(key, value, false);
        }
        memoryManager.stopThread();
        return result.putIfAbsent;
    }

    private boolean rebalancePutIfAbsent(Chunk c,
                                         Object key,
                                         Consumer<KeyInfo> keyCreator,
                                         Function<Object, Integer> keyCapacityCalculator,
                                         Consumer<ByteBuffer> valueCreator,
                                         int valueCapacity) {
        Rebalancer.RebalanceResult result = rebalance(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, null, Operation.PUT_IF_ABSENT);
        assert result != null;
        if (!result.success) { // rebalance helped put
            return putIfAbsent(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, false);
        }
        memoryManager.stopThread();
        return result.putIfAbsent;
    }

    private void rebalancePut(Chunk c, ByteBuffer key, ByteBuffer value) {
        Rebalancer.RebalanceResult result = rebalance(c, key, value, null, Operation.PUT);
        assert result != null;
        if (result.success) { // rebalance helped put
            return;
        }
        put(key, value);
    }

    private void rebalancePut(Chunk c,
                              Object key,
                              Consumer<KeyInfo> keyCreator,
                              Function<Object, Integer> keyCapacityCalculator,
                              Consumer<ByteBuffer> valueCreator,
                              int valueCapacity) {
        Rebalancer.RebalanceResult result = rebalance(c, key, keyCreator, keyCapacityCalculator, valueCreator,
                valueCapacity, null, Operation.PUT);
        assert result != null;
        if (result.success) { // rebalance helped put
            return;
        }
        put(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
    }

    private void rebalanceCompute(Chunk c, ByteBuffer key, ByteBuffer value, Supplier<ByteBuffer> constructor, Consumer<WritableOakBuffer> function) {
        Rebalancer.RebalanceResult result = rebalance(c, key, value, function, Operation.COMPUTE);
        assert result != null;
        if (result.success) { // rebalance helped compute
            return;
        }
        putIfAbsentComputeIfPresent(key, constructor, function);
    }

    private void rebalanceCompute(Chunk c,
                                  Object key,
                                  Consumer<KeyInfo> keyCreator,
                                  Function<Object, Integer> keyCapacityCalculator,
                                  Consumer<ByteBuffer> valueCreator,
                                  int valueCapacity,
                                  Consumer<WritableOakBuffer> function) {
        Rebalancer.RebalanceResult result = rebalance(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function, Operation.COMPUTE);
        assert result != null;
        if (result.success) { // rebalance helped compute
            return;
        }
        putIfAbsentComputeIfPresent(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function);
    }

    private boolean rebalanceRemove(Chunk c, Object key) {
        Rebalancer.RebalanceResult result = rebalance(c, key, null, null, Operation.REMOVE);
        assert result != null;
        return result.success;
    }

    /*-------------- oak.OakMap Methods --------------*/

    @Override
    public void put(ByteBuffer key, ByteBuffer value) {
        if (key == null || value == null || key.remaining() == 0 || value.remaining() == 0) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        // find chunk matching key
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);


        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            lookUp.handle.put(value, memoryManager);
            memoryManager.stopThread();
            return;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            put(key, value);
            memoryManager.stopThread();
            return;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalancePut(c, key, value);
            memoryManager.stopThread();
            return;
        }

        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                rebalancePut(c, key, value);
                memoryManager.stopThread();
                return;
            }
            int prevEi = c.linkEntry(ei, key, true);
            if (prevEi != ei) {
                ei = prevEi;
                prevHi = c.getHandleIndex(prevEi);
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            rebalancePut(c, key, value);
            memoryManager.stopThread();
            return;
        }

        c.writeValue(hi, valueFactory.createValue(value, memoryManager)); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT, ei, hi, prevHi, null);

        // publish put
        if (!c.publish(opData)) {
            rebalancePut(c, key, value);
            memoryManager.stopThread();
            return;
        }

        // set pointer to value
        c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();

    }

    @Override
    public void put(Object key,
                    Consumer<KeyInfo> keyCreator,
                    Function<Object, Integer> keyCapacityCalculator,
                    Consumer<ByteBuffer> valueCreator,
                    int valueCapacity) {
        if (key == null || keyCreator == null || keyCapacityCalculator == null ||
                valueCreator == null || valueCapacity == 0) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        // find chunk matching key
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);


        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            lookUp.handle.put(valueCreator, valueCapacity, memoryManager);
            memoryManager.stopThread();
            return;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, null,
                    0, null, Operation.NO_OP);
            put(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
            memoryManager.stopThread();
            return;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalancePut(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
            memoryManager.stopThread();
            return;
        }

        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key, keyCreator, keyCapacityCalculator);
            if (ei == -1) {
                rebalancePut(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
                memoryManager.stopThread();
                return;
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                ei = prevEi;
                prevHi = c.getHandleIndex(prevEi);
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            rebalancePut(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
            memoryManager.stopThread();
            return;
        }

        c.writeValue(hi, valueFactory.createValue(valueCreator, valueCapacity, memoryManager)); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT, ei, hi, prevHi, null);

        // publish put
        if (!c.publish(opData)) {
            rebalancePut(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
            memoryManager.stopThread();
            return;
        }

        // set pointer to value
        c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();
    }

    @Override
    public boolean putIfAbsent(ByteBuffer key, ByteBuffer value) {
        return putIfAbsent(key, value, true);
    }

    public boolean putIfAbsent(ByteBuffer key, ByteBuffer value, boolean startThread) {
        if (key == null || value == null || key.remaining() == 0 || value.remaining() == 0) {
            throw new NullPointerException();
        }

        if (startThread) {
            memoryManager.startThread();
        }

        // find chunk matching key
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);

        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            memoryManager.stopThread();
            return false;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            return putIfAbsent(key, value, false);
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            return rebalancePutIfAbsent(c, key, value);
        }


        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                return rebalancePutIfAbsent(c, key, value);
            }
            int prevEi = c.linkEntry(ei, key, true);
            if (prevEi != ei) {
                Handle handle = c.getHandle(prevEi);
                if (handle == null) {
                    ei = prevEi;
                    prevHi = c.getHandleIndex(prevEi);
                } else {
                    memoryManager.stopThread();
                    return false;
                }
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            return rebalancePutIfAbsent(c, key, value);
        }

        c.writeValue(hi, valueFactory.createValue(value, memoryManager)); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT_IF_ABSENT, ei, hi, prevHi, null);

        // publish put
        if (!c.publish(opData)) {
            return rebalancePutIfAbsent(c, key, value);
        }

        // set pointer to value
        boolean ret = c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();
        return ret;
    }

    @Override
    public boolean putIfAbsent(Object key,
                               Consumer<KeyInfo> keyCreator,
                               Function<Object, Integer> keyCapacityCalculator,
                               Consumer<ByteBuffer> valueCreator,
                               int valueCapacity) {
        return putIfAbsent(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, true);
    }

    public boolean putIfAbsent(Object key,
                               Consumer<KeyInfo> keyCreator,
                               Function<Object, Integer> keyCapacityCalculator,
                               Consumer<ByteBuffer> valueCreator,
                               int valueCapacity,
                               boolean startThread) {
        if (key == null || keyCreator == null || keyCapacityCalculator == null ||
                valueCreator == null || valueCapacity <= 0) {
            throw new NullPointerException();
        }

        if (startThread) {
            memoryManager.startThread();
        }

        // find chunk matching key
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);

        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            memoryManager.stopThread();
            return false;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            return putIfAbsent(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, false);
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            return rebalancePutIfAbsent(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
        }


        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key, keyCreator, keyCapacityCalculator);
            if (ei == -1) {
                return rebalancePutIfAbsent(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                Handle handle = c.getHandle(prevEi);
                if (handle == null) {
                    ei = prevEi;
                    prevHi = c.getHandleIndex(prevEi);
                } else {
                    memoryManager.stopThread();
                    return false;
                }
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            return rebalancePutIfAbsent(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
        }

        c.writeValue(hi, valueFactory.createValue(valueCreator, valueCapacity, memoryManager)); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.PUT_IF_ABSENT, ei, hi, prevHi, null);

        // publish put
        if (!c.publish(opData)) {
            return rebalancePutIfAbsent(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
        }

        // set pointer to value
        boolean ret = c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();
        return ret;
    }

    @Override
    public void  putIfAbsentComputeIfPresent(ByteBuffer key, Supplier<ByteBuffer> constructor, Consumer<WritableOakBuffer> function) {
        if (key == null || key.remaining() == 0 || constructor == null || function == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        // find chunk matching key
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);


        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            lookUp.handle.compute(function, memoryManager);
            memoryManager.stopThread();
            return;
        }

        ByteBuffer value = constructor.get();
        if (value == null || value.remaining() == 0) {
            throw new NullPointerException();
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, Operation.NO_OP);
            putIfAbsentComputeIfPresent(key, constructor, function);
            memoryManager.stopThread();
            return;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalanceCompute(c, key, value, constructor, function);
            memoryManager.stopThread();
            return;
        }

        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key);
            if (ei == -1) {
                rebalanceCompute(c, key, value, constructor, function);
                memoryManager.stopThread();
                return;
            }
            int prevEi = c.linkEntry(ei, key, true);
            if (prevEi != ei) {
                Handle handle = c.getHandle(prevEi);
                if (handle == null) {
                    ei = prevEi;
                    prevHi = c.getHandleIndex(prevEi);
                } else {
                    handle.compute(function, memoryManager);
                    memoryManager.stopThread();
                    return;
                }
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            rebalanceCompute(c, key, value, constructor, function);
            memoryManager.stopThread();
            return;
        }

        c.writeValue(hi, valueFactory.createValue(value, memoryManager)); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.COMPUTE, ei, hi, prevHi, function);

        // publish put
        if (!c.publish(opData)) {
            rebalanceCompute(c, key, value, constructor, function);
            memoryManager.stopThread();
            return;
        }

        // set pointer to value
        c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();

    }

    @Override
    public void putIfAbsentComputeIfPresent(Object key,
                                            Consumer<KeyInfo> keyCreator,
                                            Function<Object, Integer> keyCapacityCalculator,
                                            Consumer<ByteBuffer> valueCreator,
                                            int valueCapacity,
                                            Consumer<WritableOakBuffer> function) {
        if (key == null || keyCreator == null || keyCapacityCalculator == null ||
                valueCreator == null || valueCapacity <= 0 || function == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        // find chunk matching key
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);


        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp != null && lookUp.handle != null) {
            lookUp.handle.compute(function, memoryManager);
            memoryManager.stopThread();
            return;
        }

        // if chunk is frozen or infant, we can't add to it
        // we need to help rebalancer first, then proceed
        Chunk.State state = c.state();
        if (state == Chunk.State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator(), null, null, null, null, 0, null, Operation.NO_OP);
            putIfAbsentComputeIfPresent(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function);
            memoryManager.stopThread();
            return;
        }
        if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
            rebalanceCompute(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function);
            memoryManager.stopThread();
            return;
        }

        int ei = -1;
        int prevHi = -1;
        if (lookUp != null) {
            assert lookUp.handle == null;
            ei = lookUp.entryIndex;
            assert ei > 0;
            prevHi = lookUp.handleIndex;
        }

        if (ei == -1) {
            ei = c.allocateEntryAndKey(key, keyCreator, keyCapacityCalculator);
            if (ei == -1) {
                rebalanceCompute(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function);
                memoryManager.stopThread();
                return;
            }
            int prevEi = c.linkEntry(ei, true, key);
            if (prevEi != ei) {
                Handle handle = c.getHandle(prevEi);
                if (handle == null) {
                    ei = prevEi;
                    prevHi = c.getHandleIndex(prevEi);
                } else {
                    handle.compute(function, memoryManager);
                    memoryManager.stopThread();
                    return;
                }
            }
        }

        int hi = c.allocateHandle(handleFactory);
        if (hi == -1) {
            rebalanceCompute(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function);
            memoryManager.stopThread();
            return;
        }

        c.writeValue(hi, valueFactory.createValue(valueCreator, valueCapacity, memoryManager)); // write value in place

        Chunk.OpData opData = new Chunk.OpData(Operation.COMPUTE, ei, hi, prevHi, function);

        // publish put
        if (!c.publish(opData)) {
            rebalanceCompute(c, key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function);
            memoryManager.stopThread();
            return;
        }

        // set pointer to value
        c.pointToValue(opData);

        c.unpublish(opData);

        checkRebalance(c);

        memoryManager.stopThread();
    }

    @Override
    public void remove(Object key) {
        if (key == null) {
            throw new NullPointerException();
        }

        boolean logical = true;
        Handle prev = null;

        memoryManager.startThread();

        while (true) {

            // find chunk matching key
            Chunk c = skiplist.floorEntry(key).getValue();
            c = iterateChunks(c, key);

            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp != null && logical) {
                prev = lookUp.handle; // remember previous handle
            }
            if (!logical && lookUp != null && prev != lookUp.handle) {
                memoryManager.stopThread();
                return;  // someone else used this entry
            }

            if (lookUp == null || lookUp.handle == null) {
                memoryManager.stopThread();
                return;
            }

            if (logical) {
                if (!lookUp.handle.remove(memoryManager)) {
                    memoryManager.stopThread();
                    return;
                }
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            Chunk.State state = c.state();
            if (state == Chunk.State.INFANT) {
                // the infant is already connected so rebalancer won't add this put
                rebalance(c.creator(), null, null, null, Operation.NO_OP);
                logical = false;
                continue;
            }
            if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
                if (!rebalanceRemove(c, key)) {
                    logical = false;
                    continue;
                }
                memoryManager.stopThread();
                return;
            }


            assert lookUp.entryIndex > 0;
            assert lookUp.handleIndex > 0;
            Chunk.OpData opData = new Chunk.OpData(Operation.REMOVE, lookUp.entryIndex, -1, lookUp.handleIndex, null);

            // publish
            if (!c.publish(opData)) {
                if (!rebalanceRemove(c, key)) {
                    logical = false;
                    continue;
                }
                memoryManager.stopThread();
                return;
            }

            // set pointer to value
            c.pointToValue(opData);

            c.unpublish(opData);

            checkRebalance(c);

            memoryManager.stopThread();

            return;

        }
    }

    @Override
    public OakBuffer get(Object key) {
        if (key == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);

        Chunk.LookUp lookUp = c.lookUp(key);
        memoryManager.stopThread();
        return lookUp == null || lookUp.handle == null ? null : new OakBufferImpl(lookUp.handle);
    }

    @Override
    public <T> T getTransformation(Object key, Function<ByteBuffer,T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();
        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);

        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) {
            return null;
        }

        T transformation;
        lookUp.handle.readLock.lock();
        try {
            ByteBuffer value = lookUp.handle.getImmutableByteBuffer();
            transformation = transformer.apply(value);
        } finally {
            lookUp.handle.readLock.unlock();
            memoryManager.stopThread();
        }
        return transformation;
    }

    public <T> T getMinKeyTransformation(Function<ByteBuffer,T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }
        memoryManager.startThread();
        Chunk c = skiplist.firstEntry().getValue();
        ByteBuffer minKey = c.readMinKey();
        T transformation = transformer.apply(minKey);
        memoryManager.stopThread();
        return transformation;
    }

    public <T> T getMaxKeyTransformation(Function<ByteBuffer,T> transformer) {
        memoryManager.startThread();
        Chunk c = skiplist.lastEntry().getValue();
        Chunk next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }

        ByteBuffer maxKey = c.readMaxKey();
        T transformation = transformer.apply(maxKey);
        memoryManager.stopThread();
        return transformation;
    }

    @Override
    public boolean computeIfPresent(Object key, Consumer<WritableOakBuffer> function) {
        if (key == null || function == null) {
            throw new NullPointerException();
        }

        memoryManager.startThread();

        Chunk c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);

        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.handle == null) return false;

        memoryManager.stopThread();

        lookUp.handle.compute(function, memoryManager);
        return true;
    }

    @Override
    public OakMap subMap(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive) {
        if (fromKey == null || toKey == null)
            throw new NullPointerException();
        return new SubOakMap(this, fromKey, fromInclusive, toKey, toInclusive, false);
    }

    @Override
    public OakMap headMap(Object toKey, boolean inclusive) {
        if (toKey == null)
            throw new NullPointerException();
        return new SubOakMap(this, null, false, toKey, inclusive, false);
    }

    @Override
    public OakMap tailMap(Object fromKey, boolean inclusive) {
        if (fromKey == null)
            throw new NullPointerException();
        return new SubOakMap(this, fromKey, inclusive, null, false, false);
    }

    @Override
    public OakMap descendingMap() {
        OakMap dm = descendingMap;
        return (dm != null) ? dm : (descendingMap = new SubOakMap
                (this, null, false, null, false, true));
    }

    /*-------------- Iterators --------------*/

    /**
     * Base of iterator classes:
     */
    abstract class Iter<T> implements CloseableIterator<T> {

        /**
         * the next node to return from next();
         */
        Chunk nextChunk;
        Chunk.AscendingIter nextChunkIter;
        int next;
        /**
         * Cache of next value field to maintain weak consistency
         */
        OakBuffer nextValue;

        /**
         * Initializes ascending iterator for entire range.
         */
        Iter() {
            memoryManager.startThread();
            next = Chunk.NONE;
            nextValue = null;
            initChunk();
            findNextKey();
        }

        @Override
        public void close() {
            memoryManager.stopThread();
        }

        public final boolean hasNext() {
            return next != Chunk.NONE;
        }

        /**
         * Advances next to higher entry.
         */
        void advance() {
            if (next == Chunk.NONE)
                throw new NoSuchElementException();
            findNextKey();
        }

        /**
         * used only to init the iterator
         */
        private void initChunk() {
//            ByteBuffer b = constructMinKeyBuffer();
            nextChunk = skiplist.floorEntry(minKey).getValue();
            if (nextChunk == null) {
                nextChunkIter = null;
            } else {
                nextChunkIter = nextChunk.ascendingIter();
            }
        }

        /**
         * sets nextChunk, nextChunkIter, next and nextValue to the next key
         * if such a key doesn't exists then next is set to oak.Chunk.NONE and nextValue is set to null
         */
        private void findNextKey() {
            if (nextChunkIter == null) {
                return;
            }
            // find first chunk that has a valid key (chunks can have only removed keys)
            while (!nextChunkIter.hasNext()) {
                nextChunk = nextChunk.next.getReference(); // try next chunk
                if (nextChunk == null) { // there is no next chunk
                    next = Chunk.NONE;
                    nextValue = null;
                    return;
                }
                nextChunkIter = nextChunk.ascendingIter();
            }

            next = nextChunkIter.next();

            // set next value
            Handle h = nextChunk.getHandle(next);
            if (h != null) {
                nextValue = new OakBufferImpl(h);
            } else {
                nextValue = null;
            }

        }

        ByteBuffer getKey(int ki, Chunk c) {
            return c.readKey(ki);
        }

    }

    class ValueIterator extends Iter<OakBuffer> {

        public OakBuffer next() {
            OakBuffer v = nextValue;
            advance();
            return v;
        }
    }

    class EntryIterator extends Iter<Map.Entry<ByteBuffer, OakBuffer>> {

        public Map.Entry<ByteBuffer, OakBuffer> next() {
            int n = next;
            Chunk c = nextChunk;
            OakBuffer v = nextValue;
            advance();
            ByteBuffer key = getKey(n, c);
            key = key.slice(); // TODO can I get rid of this?
            return new AbstractMap.SimpleImmutableEntry<>(key, v);
        }
    }

    class KeyIterator extends Iter<ByteBuffer> {

        public ByteBuffer next() {
            int n = next;
            Chunk c = nextChunk;
            advance();
            ByteBuffer key = getKey(n, c);
            key = key.slice(); // TODO can I get rid of this?
            return key;
        }
    }

    class ValueTransformIterator<T> extends Iter<T> {

        Function<ByteBuffer, T> transformer;

        public ValueTransformIterator(Function<ByteBuffer, T> transformer) {
            super();
            this.transformer = transformer;
        }

        public T next() {
            T transformation = null;
            Handle handle = nextChunk.getHandle(next);
            handle.readLock.lock();
            try {
                ByteBuffer value = handle.getImmutableByteBuffer();
                transformation = transformer.apply(value);
            } finally {
                handle.readLock.unlock();
            }
            advance();
            return transformation;
        }
    }

    class EntryTransformIterator<T> extends Iter<T> {

        Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

        public EntryTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            super();
            this.transformer = transformer;
        }

        public T next() {
            T transformation = null;
            Handle handle = nextChunk.getHandle(next);
            ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
            handle.readLock.lock();
            try {
                ByteBuffer value = handle.getImmutableByteBuffer();
                Map.Entry<ByteBuffer, ByteBuffer> entry = new AbstractMap.SimpleImmutableEntry<>(key, value);
                transformation = transformer.apply(entry);
            } finally {
                handle.readLock.unlock();
            }
            advance();
            return transformation;
        }
    }

    class KeyTransformIterator<T> extends Iter<T> {

        Function<ByteBuffer, T> transformer;

        public KeyTransformIterator(Function<ByteBuffer, T> transformer) {
            super();
            this.transformer = transformer;
        }

        public T next() {
            ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
            return transformer.apply(key);
        }
    }

// Factory methods for iterators

    @Override
    public CloseableIterator<OakBuffer> valuesIterator() {
        return new ValueIterator();
    }

    @Override
    public CloseableIterator<Map.Entry<ByteBuffer, OakBuffer>> entriesIterator() {
        return new EntryIterator();
    }

    @Override
    public CloseableIterator<ByteBuffer> keysIterator() {
        return new KeyIterator();
    }

    @Override
    public <T> CloseableIterator<T> valuesTransformIterator(Function<ByteBuffer,T> transformer) {
        return new ValueTransformIterator<T>(transformer);
    }

    @Override
    public <T> CloseableIterator<T> entriesTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
        return new EntryTransformIterator<T>(transformer);
    }

    @Override
    public <T> CloseableIterator<T> keysTransformIterator(Function<ByteBuffer,T> transformer) {
        return new KeyTransformIterator<T>(transformer);
    }

    /* ---------------- Sub Map -------------- */

    public class SubOakMap implements OakMap {
        /**
         * underlying map
         */
        private final OakMapOffHeapImpl oak;
        /**
         * lower bound key, or null if from start
         */
        private final Object lo;
        /**
         * upper bound key, or null if to end
         */
        private final Object hi;
        /**
         * inclusion flag for lo
         */
        private final boolean loInclusive;
        /**
         * inclusion flag for hi
         */
        private final boolean hiInclusive;
        /**
         * direction
         */
        private final boolean isDescending;

        /**
         * Creates a new submap, initializing all fields.
         */
        public SubOakMap(OakMapOffHeapImpl oak,
                         Object fromKey, boolean fromInclusive,
                         Object toKey, boolean toInclusive,
                  boolean isDescending) {
            if (fromKey != null && toKey != null &&
                    oak.comparator.compare(fromKey, toKey) > 0)
                throw new IllegalArgumentException("inconsistent range");
            this.oak = oak;
            this.lo = fromKey;
            this.hi = toKey;
            this.loInclusive = fromInclusive;
            this.hiInclusive = toInclusive;
            this.isDescending = isDescending;
        }

        /*-------------- Utilities --------------*/

        int keyCompare(Object k1, Object k2) {
            return oak.comparator.compare(k1, k2);
        }

        boolean tooLow(Object key) {
            int c;
            return (lo != null && ((c = keyCompare(key, lo)) < 0 ||
                    (c == 0 && !loInclusive)));
        }

        boolean tooHigh(Object key) {
            int c;
            return (hi != null && ((c = keyCompare(key, hi)) > 0 ||
                    (c == 0 && !hiInclusive)));
        }

        boolean inBounds(Object key) {
            return !tooLow(key) && !tooHigh(key);
        }


        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        OakMapOffHeapImpl.SubOakMap newSubOakMap(Object fromKey, boolean fromInclusive,
                                                 Object toKey, boolean toInclusive) {
            if (isDescending) { // flip senses
                // TODO look into this
                Object tk = fromKey;
                fromKey = toKey;
                toKey = tk;
                boolean ti = fromInclusive;
                fromInclusive = toInclusive;
                toInclusive = ti;
            }
            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                } else {
                    int c = keyCompare(fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                } else {
                    int c = keyCompare(toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new OakMapOffHeapImpl.SubOakMap(oak, fromKey, fromInclusive,
                    toKey, toInclusive, isDescending);
        }

        /*-------------- oak.OakMap Methods --------------*/

        @Override
        public void put(ByteBuffer key, ByteBuffer value) {
            oak.put(key, value);
        }

        @Override
        public void put(Object key,
                        Consumer<KeyInfo> keyCreator,
                        Function<Object, Integer> keyCapacityCalculator,
                        Consumer<ByteBuffer> valueCreator,
                        int valueCapacity) {
            oak.put(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
        }

        @Override
        public boolean putIfAbsent(ByteBuffer key, ByteBuffer value) {
            return oak.putIfAbsent(key, value);
        }

        @Override
        public boolean putIfAbsent(Object key,
                                   Consumer<KeyInfo> keyCreator,
                                   Function<Object, Integer> keyCapacityCalculator,
                                   Consumer<ByteBuffer> valueCreator,
                                   int valueCapacity) {
            return oak.putIfAbsent(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity);
        }

        @Override
        public void putIfAbsentComputeIfPresent(ByteBuffer key, Supplier<ByteBuffer> constructor, Consumer<WritableOakBuffer> function) {
            oak.putIfAbsentComputeIfPresent(key, constructor, function);
        }

        @Override
        public void putIfAbsentComputeIfPresent(Object key,
                                                Consumer<KeyInfo> keyCreator,
                                                Function<Object, Integer> keyCapacityCalculator,
                                                Consumer<ByteBuffer> valueCreator,
                                                int valueCapacity,
                                                Consumer<WritableOakBuffer> function) {
            oak.putIfAbsentComputeIfPresent(key, keyCreator, keyCapacityCalculator, valueCreator, valueCapacity, function);
        }

        @Override
        public void remove(Object key) {
            if (inBounds(key)) {
                oak.remove(key);
            }
        }

        @Override
        public OakBuffer get(Object key) {
            if (key == null) throw new NullPointerException();
            return (!inBounds(key)) ? null : oak.get(key);
        }

        @Override
        public <T> T getTransformation(Object key, Function<ByteBuffer,T> transformer) {
            if (key == null || transformer == null) throw new NullPointerException();
            return (!inBounds(key)) ? null : oak.getTransformation(key, transformer);
        }

        @Override
        public boolean computeIfPresent(Object key, Consumer<WritableOakBuffer> function) {
            return oak.computeIfPresent(key, function);
        }

        @Override
        public OakMap subMap(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubOakMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
        public OakMap headMap(Object toKey, boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubOakMap(null, false, toKey, inclusive);
        }

        @Override
        public OakMap tailMap(Object fromKey, boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubOakMap(fromKey, inclusive, null, false);
        }

        @Override
        public OakMap descendingMap() {
            return new SubOakMap(oak, lo, loInclusive,
                    hi, hiInclusive, !isDescending);
        }

        /*-------------- Iterators --------------*/

        abstract class SubOakMapIter<T> implements CloseableIterator<T> {

            /**
             * the next node to return from next();
             */
            Chunk nextChunk;
            Chunk.ChunkIter nextChunkIter;
            int next;
            /**
             * Cache of next value field to maintain weak consistency
             */
            OakBuffer nextValue;

            SubOakMapIter() {
                memoryManager.startThread();
                next = Chunk.NONE;
                nextValue = null;
                initChunk();
                findNextKeyInRange();
            }

            @Override
            public void close() {
                memoryManager.stopThread();
            }

            public final boolean hasNext() {
                return next != Chunk.NONE;
            }

            /**
             * Advances next to higher entry.
             */
            void advance() {
                if (next == Chunk.NONE)
                    throw new NoSuchElementException();
                findNextKeyInRange();
            }

            /**
             * used only to init the iterator
             */
            private void initChunk() {
                if (!isDescending) {
                    if (lo != null)
                        nextChunk = oak.skiplist.floorEntry(lo).getValue();
                    else
                        nextChunk = oak.skiplist.floorEntry(oak.minKey).getValue();
                    if (nextChunk == null) {
                        nextChunkIter = null;
                    } else {
                        nextChunkIter = lo != null ? nextChunk.ascendingIter(lo) : nextChunk.ascendingIter();
                    }
                } else {
                    nextChunk = hi != null ? oak.skiplist.floorEntry(hi).getValue()
                            : oak.skiplist.lastEntry().getValue();
                    if (nextChunk == null) {
                        nextChunkIter = null;
                    } else {
                        nextChunkIter = hi != null ? nextChunk.descendingIter(hi, hiInclusive) : nextChunk.descendingIter();
                    }
                }
            }

            /**
             * sets nextChunk, nextChunkIter, next and nextValue to the next key in range
             * if such a key doesn't exists then next is set to oak.Chunk.NONE and nextValue is set to null
             */
            private void findNextKeyInRange() {
                if (nextChunkIter == null) {
                    return;
                }
                if (!isDescending) {
                    ascend();
                } else {
                    descend();
                }
            }

            private boolean findNextChunk() {
                while (!nextChunkIter.hasNext()) { // chunks can have only removed keys
                    nextChunk = nextChunk.next.getReference(); // try next chunk
                    if (nextChunk == null) { // there is no next chunk
                        next = Chunk.NONE;
                        nextValue = null;
                        return false;
                    }
                    nextChunkIter = nextChunk.ascendingIter();
                }
                return true;
            }

            private void ascend() {
                if (!findNextChunk()) {
                    return;
                }
                // now look for first key in range
                next = nextChunkIter.next();
                ByteBuffer key = getKey(next, nextChunk);

                while (!inBounds(key)) {
                    if (tooHigh(key)) {
                        // if we reached a key that is too high then there is no key in range
                        next = Chunk.NONE;
                        nextValue = null;
                        return;
                    }
                    if (!findNextChunk()) {
                        return;
                    }
                    next = nextChunkIter.next();
                    key = getKey(next, nextChunk);
                }
                // set next value
                Handle h = nextChunk.getHandle(next);
                if (h != null) {
                    nextValue = new OakBufferImpl(h);
                } else {
                    nextValue = null;
                }
            }

            private boolean findPrevChunk() {
                while (!nextChunkIter.hasNext()) {
                    ByteBuffer minKey = nextChunk.minKey;
                    Map.Entry e = oak.skiplist.lowerEntry(minKey);
                    if (e == null) { // there no next chunk
                        assert minKey == oak.minKey;
                        next = Chunk.NONE;
                        nextValue = null;
                        return false;
                    }
                    nextChunk = (Chunk) e.getValue();
                    Chunk nextNext = nextChunk.next.getReference();
                    if (nextNext == null) {
                        nextChunkIter = nextChunk.descendingIter(minKey, false);
                        continue;
                    }
                    ByteBuffer nextMinKey = nextNext.minKey;
                    if (nextMinKey.compareTo(minKey) < 0) {
                        nextChunk = nextNext;
                        nextMinKey = nextChunk.next.getReference().minKey;
                        while (nextMinKey.compareTo(minKey) < 0) {
                            nextChunk = nextChunk.next.getReference();
                            nextMinKey = nextChunk.next.getReference().minKey;
                        }
                    }
                    nextChunkIter = nextChunk.descendingIter(minKey, false); // TODO check correctness
                }
                return true;
            }

            private void descend() {
                if (!findPrevChunk()) {
                    return;
                }
                // now look for first key in range
                next = nextChunkIter.next();
                ByteBuffer key = getKey(next, nextChunk);

                while (!inBounds(key)) {
                    if (tooLow(key)) {
                        // if we reached a key that is too low then there is no key in range
                        next = Chunk.NONE;
                        nextValue = null;
                        return;
                    }
                    if (!findPrevChunk()) {
                        return;
                    }
                    next = nextChunkIter.next();
                    key = getKey(next, nextChunk);
                }
                nextValue = nextChunk.getHandle(next); // set next value
            }

            ByteBuffer getKey(int ki, Chunk c) {
                return c.readKey(ki);
            }

        }

        class SubOakValueIterator extends SubOakMapIter<OakBuffer> {

            SubOakValueIterator() {
                super();
            }

            public OakBuffer next() {
                OakBuffer v = nextValue;
                advance();
                return v;
            }
        }

        class SubOakEntryIterator extends SubOakMapIter<Map.Entry<ByteBuffer, OakBuffer>> {

            SubOakEntryIterator() {
                super();
            }

            public Map.Entry<ByteBuffer, OakBuffer> next() {
                int n = next;
                Chunk c = nextChunk;
                OakBuffer v = nextValue;
                advance();
                return new AbstractMap.SimpleImmutableEntry<>(getKey(n, c), v);
            }
        }

        class SubOakKeyIterator extends SubOakMapIter<ByteBuffer> {

            public ByteBuffer next() {
                int n = next;
                Chunk c = nextChunk;
                advance();
                return getKey(n, c);
            }
        }

        class SubOakValueTransformIterator<T> extends SubOakMapIter<T> {

            Function<ByteBuffer, T> transformer;

            public SubOakValueTransformIterator(Function<ByteBuffer, T> transformer) {
                super();
                this.transformer = transformer;
            }

            public T next() {
                T transformation = null;
                Handle handle = nextChunk.getHandle(next);
                ByteBuffer value = handle.getImmutableByteBuffer();
                advance();
                handle.readLock.lock();
                try {
                    transformation = transformer.apply(value);
                } finally {
                    handle.readLock.unlock();
                }
                return transformation;
            }
        }

        class SubOakEntryTransformIterator<T> extends SubOakMapIter<T> {

            Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

            public SubOakEntryTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
                super();
                this.transformer = transformer;
            }

            public T next() {
                T transformation = null;
                Handle handle = nextChunk.getHandle(next);
                ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
                ByteBuffer value = handle.getImmutableByteBuffer();
                Map.Entry<ByteBuffer, ByteBuffer> entry = new AbstractMap.SimpleImmutableEntry<>(key, value);
                advance();
                handle.readLock.lock();
                try {
                    transformation = transformer.apply(entry);
                } finally {
                    handle.readLock.unlock();
                }
                return transformation;
            }
        }

        class SubOakKeyTransformIterator<T> extends SubOakMapIter<T> {

            Function<ByteBuffer, T> transformer;

            public SubOakKeyTransformIterator(Function<ByteBuffer, T> transformer) {
                super();
                this.transformer = transformer;
            }

            public T next() {
                ByteBuffer key = getKey(next, nextChunk).asReadOnlyBuffer();
                return transformer.apply(key);
            }
        }

        // Factory methods for iterators

        @Override
        public CloseableIterator<OakBuffer> valuesIterator() {
            return new SubOakValueIterator();
        }

        @Override
        public CloseableIterator<Map.Entry<ByteBuffer, OakBuffer>> entriesIterator() {
            return new SubOakEntryIterator();
        }

        @Override
        public CloseableIterator<ByteBuffer> keysIterator() {
            return new SubOakKeyIterator();
        }

        @Override
        public <T> CloseableIterator<T> valuesTransformIterator(Function<ByteBuffer,T> transformer) {
            return new SubOakValueTransformIterator<T>(transformer);
        }

        @Override
        public <T> CloseableIterator<T> entriesTransformIterator(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            return new SubOakEntryTransformIterator<T>(transformer);
        }

        @Override
        public <T> CloseableIterator<T> keysTransformIterator(Function<ByteBuffer,T> transformer) {
            return new SubOakKeyTransformIterator<T>(transformer);
        }

    }

}
