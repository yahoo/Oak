/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak.NativeAllocator;

import com.oath.oak.OakMemoryAllocator;
import com.oath.oak.OakOutOfMemoryException;
import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OakNativeMemoryAllocator implements OakMemoryAllocator {

    // When allocating n bytes and there are buffers in the free list, only free buffers of size <= n * RECLAIM_FACTOR will be recycled
    // This parameter may be tuned for performance vs off-heap memory utilization
    private static final int RECLAIM_FACTOR = 2;

    // mapping IDs to blocks allocated solely to this Allocator
    private final ConcurrentHashMap<Integer,Block> blocks = new ConcurrentHashMap<>();
    private final AtomicInteger idGenerator = new AtomicInteger(1);

    // free list of ByteBuffers which can be reused - sorted by buffer size, then by unique hash
    private Comparator<Pair<Long, Slice>> comparator =
            Comparator.<Pair<Long, Slice>, Integer>comparing(p -> (p.getValue()).getByteBuffer().remaining())
            .thenComparing(Pair::getKey);
    private final ConcurrentSkipListSet<Pair<Long,Slice>> freeList = new ConcurrentSkipListSet<>(comparator);
    private final BlocksProvider blocksProvider;
    private Block currentBlock;
    private int currentBlockID;

    // the memory allocation limit for this Allocator
    // current capacity is set as number of blocks (!) allocated for this OakMap
    // can be changed to check only according to real allocation (allocated field)
    private final long capacity;

    // number of bytes allocated for this Oak among different Blocks
    // can be calculated, but kept for easy access
    private final AtomicLong allocated = new AtomicLong(0);
    private final AtomicLong freeCounter = new AtomicLong(0);

    // flag allowing not to close the same allocator twice
    private AtomicBoolean closed = new AtomicBoolean(false);

    // constructor
    // input param: memory capacity given to this Oak. Uses default BlocksPool
    public OakNativeMemoryAllocator(long capacity) {
       this(capacity, BlocksPool.getInstance());
    }

    // A testable constructor
    OakNativeMemoryAllocator(long capacity, BlocksProvider blocksProvider) {
        this.blocksProvider = blocksProvider;
        // initially allocate one single block from pool
        // this may lazy initialize the pool and take time if this is the first call for the pool
        allocateNewCurrentBlock();
        this.capacity = capacity;
    }

    // Allocates ByteBuffer of the given size, either from freeList or (if it is still possible)
    // within current block bounds.
    // Otherwise new block is allocated within Oak memory bounds. Thread safe.
    @Override
    public ByteBuffer allocate(int size) {

        if (!freeList.isEmpty()) {
            for (Pair<Long, Slice> kv : freeList) {
                ByteBuffer bb = kv.getValue().getByteBuffer();
                if (bb.remaining() > (RECLAIM_FACTOR * size)) break;     // all remaining buffers are too big
                if (bb.remaining() >= size && freeList.remove(kv)) {
                    if (stats != null) stats.reclaim(size);
                    return bb;
                }
            }
        }

        ByteBuffer bb = null;
        // freeList is empty or there is no suitable slice
        while (bb == null) {
            try {
                bb = currentBlock.allocate(size);
            } catch (OakOutOfMemoryException e) {
                // there is no space in current block
                // may be a buffer bigger than any block is requested?
                if (size > blocksProvider.blockSize()) {
                    throw new OakOutOfMemoryException();
                }
                // does allocation of new block brings us out of capacity?
                if ((blocks.size() + 1) * blocksProvider.blockSize() > capacity) {
                    throw new OakOutOfMemoryException();
                } else {
                    // going to allocate additional block (big chunk of memory)
                    // need to be thread-safe, so not many blocks are allocated
                    // locking is actually the most reasonable way of synchronization here
                    synchronized (this) {
                        if (currentBlock.allocated() + size > currentBlock.getCapacity()) {
                          allocateNewCurrentBlock();
                        }
                    }
                }
            }
        }
        allocated.addAndGet(size);
        return bb;
    }

  // Allocates ByteBuffer of the given size, either from freeList or (if it is still possible)
  // within current block bounds.
  // Otherwise new block is allocated within Oak memory bounds. Thread safe.
  // Returns a block id of the block from which the buffer was allocated
  public Slice allocateSlice(int size) {

//    if (!freeList.isEmpty()) {
//      for (Pair<Long, Slice> kv : freeList) {
//        ByteBuffer bb = kv.getValue().getByteBuffer();
//        if (bb.remaining() > (RECLAIM_FACTOR * size)) break;     // all remaining buffers are too big
//
//        if (bb.remaining() >= size && freeList.remove(kv)) {
//          if (stats != null) stats.reclaim(size);
//          return kv.getValue();
//        }
//      }
//    }

    ByteBuffer bb = null;
    // freeList is empty or there is no suitable slice
    while (bb == null) {
      try {
        bb = currentBlock.allocate(size);
      } catch (OakOutOfMemoryException e) {
        // there is no space in current block
        // may be a buffer bigger than any block is requested?
        if (size > blocksProvider.blockSize()) {
          throw new OakOutOfMemoryException();
        }
        // does allocation of new block brings us out of capacity?
        if ((blocks.size() + 1) * blocksProvider.blockSize() > capacity) {
          throw new OakOutOfMemoryException();
        } else {
          // going to allocate additional block (big chunk of memory)
          // need to be thread-safe, so not many blocks are allocated
          // locking is actually the most reasonable way of synchronization here
          synchronized (this) {
            if (currentBlock.allocated() + size > currentBlock.getCapacity()) {
              allocateNewCurrentBlock();
            }
          }
        }
      }
    }
    allocated.addAndGet(size);
    return new Slice(currentBlockID,bb);
  }

    // Releases memory (makes it available for reuse) without other GC consideration.
    // Meaning this request should come while it is ensured none is using this memory.
    // Thread safe.
    // IMPORTANT: it is assumed free will get ByteBuffers only initially allocated from this
    // Allocator!
    @Override
    public void free(ByteBuffer bb) {
        allocated.addAndGet(-(bb.remaining()));
        if (stats != null) stats.release(bb);
        freeList.add(new Pair<>(freeCounter.getAndIncrement(), new Slice(0,bb)));
    }

  public void free(ByteBuffer bb, int blockID) {
    allocated.addAndGet(-(bb.remaining()));
    if (stats != null) stats.release(bb);
    freeList.add(new Pair<>(freeCounter.getAndIncrement(), new Slice(blockID,bb)));
  }

    // Releases all memory allocated for this Oak (should be used as part of the Oak destruction)
    // Not thread safe, should be a single thread call. (?)
    @Override
    public void close() {
        if (!closed.compareAndSet(false,true)) return;
        for (Block b : blocks.values()) {
            blocksProvider.returnBlock(b);
        }
        // no need to do anything with the free list,
        // as all free list members were residing on one of the (already released) blocks
    }

    // Returns the off-heap allocation of this OakMap
    @Override
    public long allocated() { return allocated.get(); }

    // When some buffer need to be read from a random block
    public ByteBuffer readByteBufferFromBlockID(Integer id, int pos, int length) {
        Block b = blocks.get(id);
        return b.getBuffer(pos,length);
    }


    // used only for testing
    Block getCurrentBlock() {
        return currentBlock;
    }

    // used only for testing
    int numOfAllocatedBlocks() { return blocks.size(); }

    private void allocateNewCurrentBlock() {
      Block b = blocksProvider.getBlock();
      int blockID = idGenerator.getAndIncrement();
      this.blocks.put(blockID, b);
      this.currentBlock = b;
      this.currentBlockID = blockID;
    }

    private Stats stats = null;

    public void collectStats() {
        stats = new Stats();
    }

    public Stats getStats() {
        return stats;
    }

    public class Stats {
        public int reclaimedBuffers;
        public int releasedBuffers;
        public long releasedBytes;
        public long reclaimedBytes;

        public void release(ByteBuffer bb) {
            synchronized (this) {
                releasedBuffers++;
                releasedBytes += bb.remaining();
            }
        }

        public void reclaim(int size) {
            synchronized (this) {
                reclaimedBuffers++;
                reclaimedBytes += size;
            }
        }
    }

    public static class Slice {
      final int blockID;
      final ByteBuffer buffer;

      Slice (int blockID, ByteBuffer buffer) {
        this.blockID = blockID;
        this.buffer = buffer;
      }

      public ByteBuffer getByteBuffer() {return buffer;}
      public int getBlockID() {return blockID;}
    }

}

