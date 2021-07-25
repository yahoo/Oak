/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// the array of pointers to HashChunks according to keyHash most significant bits
class FirstLevelHashArray<K, V> {

    private AtomicReferenceArray<HashChunk<K, V>> chunks;
    private int msbForFirstLevelHash;
    private UnionCodec hashIndexCodec;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();


    FirstLevelHashArray(int msbForFirstLevelHash, AtomicInteger externalSize, MemoryManager vMM,
        MemoryManager kMM, OakComparator<K> comparator, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer, boolean resize) {

        // the key hash (int) separation between MSB for first level ans LSB for second level,
        // to be used by hash array and all chunks, until resize
        this.hashIndexCodec =
            new UnionCodec(UnionCodec.INVALID_BIT_SIZE, // the size of the first, as these are LSBs
                msbForFirstLevelHash, Integer.SIZE); // the second (MSB) will be msbForFirstLevelHash

        // the size of the hash array is defined by number of MSBs to be used from key hash
        int arraySize = (int) (Math.pow(2, msbForFirstLevelHash) + 1); // +1 to round up
        this.chunks = new AtomicReferenceArray(arraySize);
        this.msbForFirstLevelHash = msbForFirstLevelHash;

        if (!resize) {
            // initiate chunks
            for (int i = 0; i < msbForFirstLevelHash; i++) {
                HashChunk<K, V> c =
                    new HashChunk(calculateChunkSize(), externalSize, vMM, kMM, comparator,
                        keySerializer, valueSerializer, hashIndexCodec);
                this.chunks.lazySet(i, c);
            }
        }

    }

    private int calculateChunkSize() {
        // least significant bits remaining
        int lsbForSecondLevel = Integer.SIZE - msbForFirstLevelHash;
        // size of HashChunk should be 2^lsbForSecondLevel
        return (int) (Math.pow(2, lsbForSecondLevel) + 1); // +1 to round up
    }

    private int calculateKeyHash(K key) {
        int hashKey = key.hashCode(); // hash can be positive, zero or negative
        return Math.abs(hashKey); // EntryHashSet doesn't accept negative hashes
    }

    private int calculateHashArrayIdx(K key) {
        // second and not first, because these are actually the most significant bits
        return hashIndexCodec.getSecond(calculateKeyHash(key));
    }

    HashChunk<K, V> findChunk (K key) {
        return chunks.get(calculateHashArrayIdx(key));
    }

    void updateChunkInIdx(int idx, HashChunk<K, V> oldChunk, HashChunk<K, V> newChunk) {
        lock.readLock().lock();
        this.chunks.compareAndSet(idx, oldChunk, newChunk);
        lock.readLock().unlock();
    }

    void resize(AtomicInteger externalSize, MemoryManager vMM,
        MemoryManager kMM, OakComparator<K> comparator, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer) {

        lock.writeLock().lock(); // waiting for and stopping all concurrent chunk updates

        this.msbForFirstLevelHash = this.msbForFirstLevelHash + 1;

        // the key hash (int) separation between MSB for first level ans LSB for second level,
        // to be used by hash array and all chunks, until resize
        this.hashIndexCodec =
            new UnionCodec(UnionCodec.INVALID_BIT_SIZE, // the size of the first, as these are LSBs
                msbForFirstLevelHash, Integer.SIZE); // the second (MSB) will be msbForFirstLevelHash

        // the size of the hash array is defined by number of MSBs to be used from key hash
        int arraySize = (int) (Math.pow(2, msbForFirstLevelHash) + 1); // +1 to round up
        AtomicReferenceArray<HashChunk<K, V>> newChunks = new AtomicReferenceArray(arraySize);

        // copy old chunks with double referencing (looping over old chunks indexing)
        for (int i = 0; i < msbForFirstLevelHash - 1; i++) {
            // newChunks[2i] = newChunks[2i+1] = oldChunks[i]
            newChunks.lazySet(2 * i, this.chunks.get(i));
            newChunks.lazySet(2 * i + 1, this.chunks.get(i));
        }

        this.chunks = newChunks; // not atomic replace due to lock
        lock.writeLock().unlock();
    }
}
