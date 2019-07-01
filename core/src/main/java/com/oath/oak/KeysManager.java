/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

class KeysManager<K> {

    private final MemoryManager memoryManager;
    private final OakSerializer<K> keySerializer;
    private final Logger log = Logger.getLogger(KeysManager.class.getName());
    private final ThreadIndexCalculator threadIndexCalculator;
    private ByteBuffer[] byteBufferPerThread;

    KeysManager(MemoryManager memoryManager, OakSerializer<K> keySerializer,
        ThreadIndexCalculator threadIndexCalculator) {
      this.threadIndexCalculator = threadIndexCalculator;
      this.keySerializer = keySerializer;
      this.memoryManager = memoryManager;
      this.byteBufferPerThread = new ByteBuffer[ThreadIndexCalculator.MAX_THREADS];
    }



//    public void writeKey(K key, int ki) {
//        ByteBuffer byteBuffer = keys.duplicate();
//        byteBuffer.position(keys.position() + ki);
//        byteBuffer.limit(keys.position() + ki + keySerializer.calculateSize(key));
//        byteBuffer = byteBuffer.slice();
//        // byteBuffer is set so it protects us from the overwrites of the serializer
//        keySerializer.serialize(key, byteBuffer);
//    }

    public OakNativeMemoryAllocator.Slice writeSeparateKey(K key) {
        OakNativeMemoryAllocator.Slice s
            = memoryManager.allocateSlice(keySerializer.calculateSize(key));
        // byteBuffer.slice() is set so it protects us from the overwrites of the serializer
        keySerializer.serialize(key, s.getByteBuffer().slice());
        return s;
    }

    //public ByteBuffer getKeys() {
    //    return keys;
    //}

    public void release() {
       // memoryManager.releaseKeys(keys);
    }

  /**
   * Reads a key given the entry index. Key is returned via reusable thread-local ByteBuffer.
   * There is no copy just a special ByteBuffer for a single key.
   * The thread-local ByteBuffer can be reused by different threads, however as long as
   * a thread is invoked the ByteBuffer is related solely to this thread.
   */
  ByteBuffer readKey(int entryIndex, int[] entries) {
    if (entryIndex == Chunk.NONE) {
      return null;
    }

    int blockID = Chunk.getEntryField(entryIndex, Chunk.OFFSET_KEY_BLOCK, entries);
    int keyPosition = Chunk.getEntryField(entryIndex, Chunk.OFFSET_KEY_POSITION, entries);
    int length = Chunk.getEntryField(entryIndex, Chunk.OFFSET_KEY_LENGTH, entries);

    int idx = threadIndexCalculator.getIndex();
    if (byteBufferPerThread[idx] == null) {
      byteBufferPerThread[idx] = memoryManager.getByteBufferOfBlockID(blockID);
    }

    ByteBuffer bbThread = byteBufferPerThread[idx];
    bbThread.position(keyPosition);
    bbThread.limit(keyPosition + length);

    return bbThread;
  }
}
