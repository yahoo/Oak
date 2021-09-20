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
    // defaults
    public static final int HASH_CHUNK_NUM_DEFAULT = 1024;

    private AtomicReferenceArray<HashChunk<K, V>> chunks;
    private int msbForFirstLevelHash;
    private UnionCodec hashIndexCodec;
    // lock protects single chunk rebalance vs hash array resize
    private final ReadWriteLock lock = new ReentrantReadWriteLock();


    /**
     * Create the array referencing to all empty chunks. Multiple array references
     * (exactly `multipleReferenceNum`) are going to point to the same chunk, next `multipleReferenceNum`
     * num of pointers are going to point to another chunk and so on.
     * Other creation patterns (like all the hash array entries referencing the same chunk) are possible.
     * Majority of the parameters are needed for chunk creation.
     *  @param msbForFirstLevelHash number of most significant bits to be used to calculate the
     *                             index in the hash array. It also affects directly the size of the
     *                             array. It is going to be incorporated inside hashIndexCodec and
     *                             transferred to chunks.
     * @param lsbForSecondLevelHash
     * @param multipleReferenceNum number of the references to reference the same chunk,
     *                             before switching to next chunk
     *
     * The relation between FirstLevelHashArray level and Chunk level hashIndexCodecs:
     * 1. Some MSB bits of keyHash are taken to calculate the index in the FirstLevelHashArray
     *              to find Chunk C (using hashIndexCodec).
     * 2. Some LSB bits of keyHash are taken to calculate the entry index in the Chunk C
     *              (using a different hashIndexCodecForChunk).
     * 3. As those bits are not necessarily following one another, and may be even overlapping
     *              different hashIndexCodecs are created. If FirstLevelHashArray size changes,
     *              hashIndexCodec is changed, but not necessarily hashIndexCodecForChunk, and visa versa
     */
    FirstLevelHashArray(int msbForFirstLevelHash, int lsbForSecondLevelHash, AtomicInteger externalSize,
        MemoryManager vMM, MemoryManager kMM, OakComparator<K> comparator,
        OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer, int multipleReferenceNum) {

        // the key hash (int) separation between MSB for first level ans LSB for second level,
        // to be used by hash array and all chunks, until resize
        this.hashIndexCodec =
            new UnionCodec(UnionCodec.AUTO_CALCULATE_BIT_SIZE, // the size of the first, as these are LSBs
                msbForFirstLevelHash, // the second (MSB) will be msbForFirstLevelHash
                1, // and disregard the first bit as it is the sign bit and always zero
                Integer.SIZE);

        // the size of the hash array is defined by number of MSBs to be used from key hash
        int arraySize = (int) (Math.pow(2, msbForFirstLevelHash));
        this.chunks = new AtomicReferenceArray<>(arraySize);
        this.msbForFirstLevelHash = msbForFirstLevelHash;

        int currentSameRefer = multipleReferenceNum;
        HashChunk<K, V> c = null;
        UnionCodec hashIndexCodecForChunk =
            (lsbForSecondLevelHash == InternalOakHash.USE_DEFAULT_FIRST_TO_SECOND_BITS_PARTITION)
                ? this.hashIndexCodec :
                new UnionCodec(lsbForSecondLevelHash, // the size of the first, as these are LSBs
                    UnionCodec.AUTO_CALCULATE_BIT_SIZE, Integer.SIZE); // the second (MSB) will be auto-calculated
        int chunkSize = calculateChunkSize(lsbForSecondLevelHash);
        // initiate chunks
        for (int i = 0; i < chunks.length(); i++) {
            if (currentSameRefer == multipleReferenceNum) {
                c = new HashChunk<>(chunkSize, externalSize, vMM, kMM, comparator,
                    keySerializer, valueSerializer, hashIndexCodecForChunk);
            }
            this.chunks.lazySet(i, c);
            currentSameRefer--;
            if (currentSameRefer == 0) {
                currentSameRefer = multipleReferenceNum;
            }
        }
    }

    private int calculateChunkSize(int inputLsbForSecondLevel) {
        // least significant bits remaining
        int lsbForSecondLevel =
            (inputLsbForSecondLevel == InternalOakHash.USE_DEFAULT_FIRST_TO_SECOND_BITS_PARTITION)
                ? Integer.SIZE - msbForFirstLevelHash : inputLsbForSecondLevel;
        // size of HashChunk should be 2^lsbForSecondLevel
        return (int) Math.ceil(Math.pow(2, lsbForSecondLevel));
    }

    private int calculateKeyHash(K key, ThreadContext ctx) {
        return ctx.operationKeyHash;
    }

    private int calculateHashArrayIdx(K key, ThreadContext ctx) {
        // second and not first, because these are actually the most significant bits
        return hashIndexCodec.getSecond(calculateKeyHash(key, ctx));
    }

    HashChunk<K, V> findChunk(K key, ThreadContext ctx, int keyHash) {
        return chunks.get(calculateHashArrayIdx(key, ctx));
    }

    HashChunk<K, V> getChunk(int index) {
        assert 0 <= index && index < chunks.length();
        return chunks.get(index);
    }

    /**
     * Brings the chunks to their initial state without entries
     * Used when we want to empty the structure without reallocating all the objects/memory
     * Exists only for hash, as for the map there are min keys in the off-heap memory
     * and the full clear method is more subtle
     * NOT THREAD SAFE !!!
     */
    void clear() {
        for (int i = 0; i < chunks.length(); i++) {
            chunks.get(i).clear();
        }
    }

    // To be used when rebalance of chunk is in place
    void updateChunkInIdx(int idx, HashChunk<K, V> oldChunk, HashChunk<K, V> newChunk) {
        lock.readLock().lock();
        try {
            this.chunks.compareAndSet(idx, oldChunk, newChunk);
        } finally {
            lock.readLock().unlock();
        }
    }

    void resize(AtomicInteger externalSize, MemoryManager vMM,
        MemoryManager kMM, OakComparator<K> comparator, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer) {

        try {
            lock.writeLock().lock(); // waiting for and stopping all concurrent chunk updates

            this.msbForFirstLevelHash = this.msbForFirstLevelHash + 1;

            // the key hash (int) separation between MSB for first level ans LSB for second level,
            // to be used by hash array and all chunks, until resize
            this.hashIndexCodec = new UnionCodec(UnionCodec.AUTO_CALCULATE_BIT_SIZE,
                // the size of the first, as these are LSBs
                msbForFirstLevelHash, Integer.SIZE); // the second (MSB) will be msbForFirstLevelHash

            // the size of the hash array is defined by number of MSBs to be used from key hash
            int arraySize = (int) (Math.pow(2, msbForFirstLevelHash));
            AtomicReferenceArray<HashChunk<K, V>> newChunks = new AtomicReferenceArray<>(arraySize);

            // copy old chunks with double referencing (looping over old chunks indexing)
            for (int i = 0; i < msbForFirstLevelHash - 1; i++) {
                // newChunks[2i] = newChunks[2i+1] = oldChunks[i]
                newChunks.lazySet(2 * i, this.chunks.get(i));
                newChunks.lazySet(2 * i + 1, this.chunks.get(i));
            }

            this.chunks = newChunks; // not atomic replace due to lock
        } finally {
            lock.writeLock().unlock();
        }
    }
}
