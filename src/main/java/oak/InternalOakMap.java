/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.NoSuchElementException;
import java.util.AbstractMap;

public class InternalOakMap<K, V> implements AutoCloseable {

  /*-------------- Members --------------*/

  Logger log = Logger.getLogger(InternalOakMap.class.getName());
  final ConcurrentSkipListMap<Object, Chunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
  private final AtomicReference<Chunk<K, V>> head;
  private final ByteBuffer minKey;
  public final Comparator comparator;
  final OakMemoryManager memoryManager;
  private final HandleFactory handleFactory;
  private AtomicInteger size;
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  /**
   * Lazily initialized descending key set
   */
  private InternalOakMap descendingMap;

    /*-------------- Constructors --------------*/

  /**
   * init with capacity = 2g
   */

  public InternalOakMap(
          K minKey,
          Serializer<K> keySerializer,
          Serializer<V> valueSerializer,
          OakComparator<K> comparator,
          MemoryPool memoryPool,
          int chunkMaxItems,
          int chunkBytesPerItem) {

    this.size = new AtomicInteger(0);
    this.memoryManager = new OakMemoryManager(memoryPool);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

    this.comparator = new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        if (o1 instanceof ByteBuffer) {
          if (o2 instanceof ByteBuffer) {
            return comparator.compareSerializedKeys((ByteBuffer) o1, (ByteBuffer) o2);
          } else {
            return comparator.compareSerializedKeyAndKey((ByteBuffer) o1, (K) o2);
          }
        } else {
          if (o2 instanceof ByteBuffer) {
            return (-1) * comparator.compareSerializedKeyAndKey((ByteBuffer) o2, (K) o1);
          } else {
            return comparator.compareKeys((K) o1, (K) o2);
          }
        }
      }
    };

    this.minKey = ByteBuffer.allocate(this.keySerializer.calculateSize(minKey));
    this.minKey.position(0);
    this.keySerializer.serialize(minKey, this.minKey);

    this.skiplist = new ConcurrentSkipListMap<>(this.comparator);
    Chunk<K, V> head = new Chunk<K, V>(this.minKey, null, this.comparator, memoryManager, chunkMaxItems,
            chunkBytesPerItem, this.size, keySerializer, valueSerializer);
    this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
    this.head = new AtomicReference<>(head);

    this.descendingMap = null;
    this.handleFactory = new HandleFactory(true);
  }

  static int getThreadIndex() {
    // TODO use hash instead of modulo
    return (int) (Thread.currentThread().getId() % Chunk.MAX_THREADS);
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
  public long memorySize() {
    return memoryManager.pool.allocated();
  }

  public int entries() { return size.get(); }

  /*-------------- Methods --------------*/

  /**
   * finds and returns the chunk where key should be located, starting from given chunk
   */
  private Chunk<K, V> iterateChunks(Chunk<K, V> c, Object key) {
    // find chunk following given chunk (next)
    Chunk<K, V> next = c.next.getReference();

    // since skiplist isn't updated atomically in split/compaction, our key might belong in the next chunk
    // we need to iterate the chunks until we find the correct one
    while ((next != null) && (comparator.compare(next.minKey, key) <= 0)) {
      c = next;
      next = c.next.getReference();
    }

    return c;
  }

  private Rebalancer.RebalanceResult rebalance(Chunk<K, V> c, K key, V value, Consumer<ByteBuffer> computer, Operation op) {
    if (c == null) {
      assert op == Operation.NO_OP;
      return null;
    }
    Rebalancer rebalancer = new Rebalancer(c, comparator, true, memoryManager, handleFactory,
            keySerializer, valueSerializer);

    rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

    // freeze all the engaged range.
    // When completed, all update (put, next pointer update) operations on the engaged range
    // will be redirected to help the rebalance procedure
    rebalancer.freeze();

    Rebalancer.RebalanceResult result = rebalancer.createNewChunks(key, value, computer, op); // split or compact
    // if returned true then this thread was responsible for the creation of the new chunks
    // and it inserted the put

    // lists may be generated by another thread
    List<Chunk<K, V>> newChunks = rebalancer.getNewChunks();
    List<Chunk<K, V>> engaged = rebalancer.getEngagedChunks();

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
      rebalance(c, null, null, null, Operation.NO_OP);
    }
  }

  private void connectToChunkList(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {

    updateLastChild(engaged, children);

    Chunk<K, V> firstEngaged = engaged.get(0);

    // replace in linked list - we now need to find previous chunk to our chunk
    // and CAS its next to point to c1, which is the same c1 for all threads who reached this point
    // since prev might be marked (in compact itself) - we need to repeat this until successful
    while (true) {
      // start with first chunk (i.e., head)
      Map.Entry<Object, Chunk<K, V>> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

      Chunk<K, V> prev = lowerEntry != null ? lowerEntry.getValue() : null;
      Chunk<K, V> curr = (prev != null) ? prev.next.getReference() : null;

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

  private void updateLastChild(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {
    Chunk<K, V> lastEngaged = engaged.get(engaged.size() - 1);
    Chunk<K, V> nextToLast = lastEngaged.markAndGetNext(); // also marks last engaged chunk as deleted
    Chunk<K, V> lastChild = children.get(children.size() - 1);

    lastChild.next.compareAndSet(null, nextToLast, false, false);
  }

  private void updateIndexAndNormalize(List<Chunk<K, V>> engagedChunks, List<Chunk<K, V>> children) {
    Iterator<Chunk<K, V>> iterEngaged = engagedChunks.iterator();
    Iterator<Chunk<K, V>> iterChildren = children.iterator();

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

  private boolean rebalancePutIfAbsent(Chunk<K, V> c, K key, V value) {
    Rebalancer.RebalanceResult result = rebalance(c, key, value, null, Operation.PUT_IF_ABSENT);
    assert result != null;
    if (!result.success) { // rebalance helped put
      return putIfAbsent(key, value, false);
    }
    memoryManager.stopThread();
    return result.putIfAbsent;
  }

  private void rebalancePut(Chunk<K, V> c, K key, V value) {
    Rebalancer.RebalanceResult result = rebalance(c, key, value, null, Operation.PUT);
    assert result != null;
    if (result.success) { // rebalance helped put
      return;
    }
    put(key, value);
  }

  private void rebalanceCompute(Chunk<K, V> c, K key, V value, Consumer<ByteBuffer> computer) {
    Rebalancer.RebalanceResult result = rebalance(c, key, value, computer, Operation.COMPUTE);
    assert result != null;
    if (result.success) { // rebalance helped compute
      return;
    }
    putIfAbsentComputeIfPresent(key, value, computer);
  }

  private boolean rebalanceRemove(Chunk<K, V> c, K key) {
    Rebalancer.RebalanceResult result = rebalance(c, key, null, null, Operation.REMOVE);
    assert result != null;
    return result.success;
  }

  /*-------------- OakMap Methods --------------*/

  public void put(K key, V value) {
    if (key == null || value == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();

    Chunk<K, V> c = findChunk(key); // find chunk matching key
    Chunk.LookUp lookUp = c.lookUp(key);
    if (lookUp != null && lookUp.handle != null) {
      lookUp.handle.put(value, valueSerializer, memoryManager);
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
      int prevEi = c.linkEntry(ei, true, key);
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

    c.writeValue(hi, value); // write value in place

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

  public boolean putIfAbsent(K key, V value) {
    return putIfAbsent(key, value, true);
  }

  public boolean putIfAbsent(K key, V value, boolean startThread) {
    if (key == null || value == null) {
      throw new NullPointerException();
    }

    if (startThread) {
      memoryManager.startThread();
    }

    Chunk c = findChunk(key); // find chunk matching key
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
      return rebalancePutIfAbsent(c, key, value);
    }

    c.writeValue(hi, value); // write value in place

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

  public void putIfAbsentComputeIfPresent(K key, V value, Consumer<ByteBuffer> computer) {
    if (key == null || value == null || computer == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();

    Chunk c = findChunk(key); // find chunk matching key
    Chunk.LookUp lookUp = c.lookUp(key);
    if (lookUp != null && lookUp.handle != null) {
      if (lookUp.handle.compute(computer, memoryManager)) {
        // compute was successful and handle wasn't found deleted; in case
        // this handle was already found as deleted, continue to construct another handle
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
      putIfAbsentComputeIfPresent(key, value, computer);
      memoryManager.stopThread();
      return;
    }
    if (state == Chunk.State.FROZEN || state == Chunk.State.RELEASED) {
      rebalanceCompute(c, key, value, computer);
      memoryManager.stopThread();
      return;
    }

    // we come here when no key was found, which can be in 3 cases:
    // 1. no entry in the linked list at all
    // 2. entry in the linked list, but handle is not attached
    // 3. entry in the linked list, handle attached, but handle is marked deleted
    int ei = -1;
    int prevHi = -1;
    if (lookUp != null) {
      ei = lookUp.entryIndex;
      assert ei > 0;
      prevHi = lookUp.handleIndex;
    }

    if (ei == -1) {
      ei = c.allocateEntryAndKey(key);
      if (ei == -1) {
        rebalanceCompute(c, key, value, computer);
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
          if (handle.compute(computer, memoryManager)) {
            // compute was successful and handle wasn't found deleted; in case
            // this handle was already found as deleted, continue to construct another handle
            memoryManager.stopThread();
            return;
          }
        }
      }
    }

    int hi = c.allocateHandle(handleFactory);
    if (hi == -1) {
      rebalanceCompute(c, key, value, computer);
      memoryManager.stopThread();
      return;
    }

    c.writeValue(hi, value); // write value in place

    Chunk.OpData opData = new Chunk.OpData(Operation.COMPUTE, ei, hi, prevHi, computer);

    // publish put
    if (!c.publish(opData)) {
      rebalanceCompute(c, key, value, computer);
      memoryManager.stopThread();
      return;
    }

    // set pointer to value
    c.pointToValue(opData);

    c.unpublish(opData);

    checkRebalance(c);

    memoryManager.stopThread();
  }

  public void remove(K key) {
    if (key == null) {
      throw new NullPointerException();
    }

    boolean logical = true;
    Handle prev = null;

    memoryManager.startThread();

    while (true) {

      Chunk c = findChunk(key); // find chunk matching key
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

  public OakRBuffer get(K key) {
    if (key == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();
    Chunk<K, V> c = findChunk(key); // find chunk matching key
    Chunk.LookUp lookUp = c.lookUp(key);
    if (lookUp == null || lookUp.handle == null) {
      return null;
    }
    memoryManager.stopThread();
    return new OakRValueBufferImpl(lookUp.handle);
  }

  public <T> T getValueTransformation(K key, Function<ByteBuffer,T> transformer) {
    if (key == null || transformer == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();
    Chunk<K, V> c = findChunk(key); // find chunk matching key
    Chunk.LookUp lookUp = c.lookUp(key);
    if (lookUp == null || lookUp.handle == null) {
      return null;
    }

    OakRValueBufferImpl oakRValueBuffer = new OakRValueBufferImpl(lookUp.handle);
    T transformation = oakRValueBuffer.transform(transformer);
    memoryManager.stopThread();
    return transformation;
  }

  public <T> T getKeyTransformation(K key, Function<ByteBuffer,T> transformer) {
    if (key == null || transformer == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();
    Chunk<K, V> c;
    T transformation;
    do {
      c = findChunk(key); // find chunk matching key
      Chunk.LookUp lookUp = c.lookUp(key);
      if (lookUp == null || lookUp.handle == null || lookUp.entryIndex == -1) {
        memoryManager.stopThread();
        return null;
      }
      ByteBuffer serializedKey = c.readKey(lookUp.entryIndex);
      OakRKeyBufferImpl oakRKeyBuffer = new OakRKeyBufferImpl(c, serializedKey);
      transformation = oakRKeyBuffer.transform(transformer);
      if (transformation != null) {
        break;
      }
    } while (c.state() == Chunk.State.RELEASED);

    memoryManager.stopThread();
    return transformation;
  }

  public OakRBuffer getKey(K key) {
    if (key == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();
    Chunk<K, V> c = findChunk(key);
    Chunk.LookUp lookUp = c.lookUp(key);
    if (lookUp == null || lookUp.handle == null || lookUp.entryIndex == -1) {
      memoryManager.stopThread();
      return null;
    }
    ByteBuffer serializedKey = c.readKey(lookUp.entryIndex);
    memoryManager.stopThread();
    return new OakRKeyBufferImpl(c, serializedKey);
  }

  public OakRBuffer getMinKey() {
    memoryManager.startThread();
    Chunk<K, V> c = skiplist.firstEntry().getValue();
    ByteBuffer serializedMinKey = c.readMinKey();
    memoryManager.stopThread();
    return new OakRKeyBufferImpl(c, serializedMinKey);
  }

  public <T> T getMinKeyTransformation(Function<ByteBuffer,T> transformer) {
    if (transformer == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();
    Chunk<K, V> c;
    T transformation;

    do {
      c = skiplist.firstEntry().getValue();
      ByteBuffer serializedMinKey = c.readMinKey();
      OakRKeyBufferImpl oakRKeyBuffer = new OakRKeyBufferImpl(c, serializedMinKey);
      transformation = oakRKeyBuffer.transform(transformer);
      if (transformation != null) {
        break;
      }
    } while (c.state() == Chunk.State.RELEASED);

    memoryManager.stopThread();
    return transformation;
  }

  public OakRBuffer getMaxKey() {
    memoryManager.startThread();
    Chunk<K, V> c = skiplist.lastEntry().getValue();
    Chunk<K, V> next = c.next.getReference();
    // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
    // we need to iterate the chunks until we find the last one
    while (next != null) {
      c = next;
      next = c.next.getReference();
    }

    ByteBuffer serializedMaxKey = c.readMaxKey();
    memoryManager.stopThread();
    return new OakRKeyBufferImpl(c, serializedMaxKey);
  }

  public <T> T getMaxKeyTransformation(Function<ByteBuffer,T> transformer) {
    if (transformer == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();
    Chunk<K, V> c, next;
    T transformation;

    do {
      c = skiplist.lastEntry().getValue();
      next = c.next.getReference();
      // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
      // we need to iterate the chunks until we find the last one
      while (next != null) {
        c = next;
        next = c.next.getReference();
      }
      ByteBuffer serializedMaxKey = c.readMaxKey();
      OakRKeyBufferImpl oakRKeyBuffer = new OakRKeyBufferImpl(c, serializedMaxKey);
      transformation = oakRKeyBuffer.transform(transformer);
      if (transformation != null) {
        break;
      }
    } while (c.state() == Chunk.State.RELEASED);

    memoryManager.stopThread();
    return transformation;
  }

  public boolean computeIfPresent(K key, Consumer<OakWBuffer> computer) {
    if (key == null || computer == null) {
      throw new NullPointerException();
    }

    memoryManager.startThread();
    Chunk c = findChunk(key); // find chunk matching key
    Chunk.LookUp lookUp = c.lookUp(key);
    if (lookUp == null || lookUp.handle == null) return false;

    memoryManager.stopThread();

    lookUp.handle.compute(computer, memoryManager);
    return true;
  }

  // encapsulates finding of the chunk in the skip list and later chunk list traversal
  private Chunk findChunk(Object key) {
    Chunk c = skiplist.floorEntry(key).getValue();
    c = iterateChunks(c, key);
    return c;
  }

  /*-------------- Iterators --------------*/

  /**
   * Base of iterator classes:
   */
  abstract class Iter<T> implements CloseableIterator<T> {

    private final K lo;
    /**
     * upper bound key, or null if to end
     */
    private final K hi;
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
     * the next node to return from next();
     */
    Chunk<K, V> nextChunk;
    Chunk.AscendingIter nextChunkIter;
    int next;
    /**
     * Cache of next value field to maintain weak consistency
     */
    Handle nextHandle;

    /**
     * Initializes ascending iterator for entire range.
     */
    Iter(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
      if (lo != null && hi != null &&
              comparator.compare(lo, hi) > 0)
        throw new IllegalArgumentException("inconsistent range");

      this.lo = lo;
      this.loInclusive = loInclusive;
      this.hi = hi;
      this.hiInclusive = hiInclusive;
      this.isDescending = isDescending;
      memoryManager.startThread();
      next = Chunk.NONE;
      nextHandle = null;
      initChunk();
      findNextKeyInRange();
    }

    @Override
    public void close() {
      memoryManager.stopThread();
    }

    boolean tooLow(Object key) {
      int c;
      return (lo != null && ((c = comparator.compare(key, lo)) < 0 ||
              (c == 0 && !loInclusive)));
    }

    boolean tooHigh(Object key) {
      int c;
      return (hi != null && ((c = comparator.compare(key, hi)) > 0 ||
              (c == 0 && !hiInclusive)));
    }

    boolean inBounds(Object key) {
      return !tooLow(key) && !tooHigh(key);
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
          nextChunk = (Chunk<K, V>) skiplist.floorEntry(lo).getValue();
        else
          nextChunk = (Chunk<K, V>) skiplist.floorEntry(minKey).getValue();
        if (nextChunk == null) {
          nextChunkIter = null;
        } else {
          nextChunkIter = lo != null ? nextChunk.ascendingIter(lo) : nextChunk.ascendingIter();
        }
      } else {
        nextChunk = hi != null ? (Chunk<K, V>) skiplist.floorEntry(hi).getValue()
                : (Chunk<K, V>) skiplist.lastEntry().getValue();
        if (nextChunk == null) {
          nextChunkIter = null;
        } else {
          nextChunkIter = hi != null ? nextChunk.descendingIter(hi, hiInclusive) : nextChunk.descendingIter();
        }
      }
    }

    /**
     * sets nextChunk, nextChunkIter, next and nextValue to the next key
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
          nextHandle = null;
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
        nextHandle = h;
      } else {
        nextHandle = null;
      }
    }

    private boolean findNextChunk() {
      while (!nextChunkIter.hasNext()) { // chunks can have only removed keys
        nextChunk = nextChunk.next.getReference(); // try next chunk
        if (nextChunk == null) { // there is no next chunk
          next = Chunk.NONE;
          nextHandle = null;
          return false;
        }
        nextChunkIter = nextChunk.ascendingIter();
      }
      return true;
    }

    private boolean findPrevChunk() {
      while (!nextChunkIter.hasNext()) {
        ByteBuffer serializedMinKey = nextChunk.minKey;
        Map.Entry e = skiplist.lowerEntry(serializedMinKey);
        if (e == null) { // there no next chunk
          assert serializedMinKey == minKey;
          next = Chunk.NONE;
          nextHandle = null;
          return false;
        }
        nextChunk = (Chunk) e.getValue();
        Chunk nextNext = nextChunk.next.getReference();
        if (nextNext == null) {
          nextChunkIter = nextChunk.descendingIter((K) keySerializer.deserialize(serializedMinKey), false);
          continue;
        }
        ByteBuffer nextMinKey = nextNext.minKey;
        if (comparator.compare(nextMinKey, serializedMinKey) < 0) {
          nextChunk = nextNext;
          nextMinKey = nextChunk.next.getReference().minKey;
          while (comparator.compare(nextMinKey, serializedMinKey) < 0) {
            nextChunk = nextChunk.next.getReference();
            nextMinKey = nextChunk.next.getReference().minKey;
          }
        }
        nextChunkIter = nextChunk.descendingIter((K) keySerializer.deserialize(serializedMinKey), false); // TODO check correctness
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
          nextHandle = null;
          return;
        }
        if (!findPrevChunk()) {
          return;
        }
        next = nextChunkIter.next();
        key = getKey(next, nextChunk);
      }
      nextHandle = nextChunk.getHandle(next); // set next value
    }

    ByteBuffer getKey(int ki, Chunk c) {
      return c.readKey(ki);
    }

  }

  class ValueIterator extends Iter<OakRBuffer> {

    public OakRBuffer next() {
      int n = next;
      Handle handle = nextHandle;
      Chunk c = nextChunk;
      advance();
      if (handle == null)
        return null;

      return new OakRValueBufferImpl(handle);
    }
  }

  class EntryIterator extends Iter<Map.Entry<OakRBuffer, OakRBuffer>> {

    public Map.Entry<OakRBuffer, OakRBuffer> next() {
      int n = next;
      Chunk c = nextChunk;
      Handle handle = nextHandle;
      advance();
      if (handle == null)
        return null;
      ByteBuffer serializedKey = getKey(n, c);
      serializedKey = serializedKey.slice(); // TODO can I get rid of this?
      return new AbstractMap.SimpleImmutableEntry<OakRBuffer, OakRBuffer>
              (new OakRKeyBufferImpl(c, serializedKey), new OakRValueBufferImpl(handle));
    }
  }

  class KeyIterator extends Iter<OakRBuffer> {

    public OakRBuffer next() {
      int n = next;
      Chunk c = nextChunk;
      advance();
      ByteBuffer serializedKey = getKey(n, c);
      serializedKey = serializedKey.slice(); // TODO can I get rid of this?
      return new OakRKeyBufferImpl(c, serializedKey);
    }
  }

  // Factory methods for iterators

  public CloseableIterator<OakRBuffer> valuesIterator() {
    return new ValueIterator();
  }

  public CloseableIterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesIterator() {
    return new EntryIterator();
  }

  public CloseableIterator<OakRBuffer> keysIterator() {
    return new KeyIterator();
  }

}
