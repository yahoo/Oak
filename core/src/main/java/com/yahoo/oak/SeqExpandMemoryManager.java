/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


class SeqExpandMemoryManager implements MemoryManager {
    private final BlockMemoryAllocator allocator;

    /*
     * The direct reference codec encodes the reference of the slices (which are not subject to
     * memory reclamation) into a single long primitive (64 bit).
     * For the default block size (256MB), we need 28 bits to encode the offset
     * and additional 28 bits to encode the length.
     * So, the remaining 8 bits can encode the block id, which will limit the maximal number of blocks to 256.
     * Thus, the reference encoding when using the default block size (256MB) will be as follows:
     *
     *    LSB                                       MSB
     *     |     offset     |     length     | block |
     *     |     28 bit     |     28 bit     | 8 bit |
     *      0             27 28            55 56   63
     *
     * From that, we can derive that the maximal number of 1K items that can be allocated is ~128 million (2^26).
     * Note: these limitations will change for different block sizes.
     *
     */
    private final ReferenceCodecSeqExpand rcd;

    SeqExpandMemoryManager(BlockMemoryAllocator memoryAllocator) {
        assert memoryAllocator != null;
        this.allocator = memoryAllocator;
        rcd = new ReferenceCodecSeqExpand(
            BlocksPool.getInstance().blockSize(), BlocksPool.getInstance().blockSize(), memoryAllocator);
    }

    public void close() {
        allocator.close();
    }

    public long allocated() {
        return allocator.allocated();
    }

    // No-free memory manager requires no header therefore the same size as requested is allocated
    @Override
    public void allocate(Slice s, int size, boolean existing) {
        boolean allocated = allocator.allocate(s, size);
        assert allocated;
    }

    @Override
    public void release(Slice s) {
    }

    public boolean isClosed() {
        return allocator.isClosed();
    }

    /**
     * @param s         the memory slice to update with the info decoded from the reference
     * @param reference the reference to decode
     * @return true if the given allocation reference is valid, otherwise the slice is invalidated
     */
    @Override
    public boolean decodeReference(Slice s, long reference) {
        if (s.getAllocatedBlockID() == rcd.getFirst(reference)) {
            // it shows performance improvement (10%) in stream scans, when only offset of the
            // key's slice is updated upon reference decoding.
            // Slice is not invalidated between next iterator steps and all the rest information
            // in slice remains the same.
            s.updateOnSameBlock(rcd.getSecond(reference)/*offset*/, rcd.getThird(reference)/*length*/);
            return true;
        }
        if (rcd.decode(s, reference)) {
            allocator.readMemoryAddress(s);
            return true;
        }
        return false;
    }

    /**
     * @param s the memory slice, encoding of which should be returned as a an output long reference
     * @return the encoded reference
     */
    @Override
    public long encodeReference(Slice s) {
        return rcd.encode(s);
    }

    /**
     * Present the reference as it needs to be when the target is deleted
     *
     * @param reference to alter
     * @return the encoded reference
     */
    @Override
    public long alterReferenceForDelete(long reference) {
        return rcd.alterForDelete(reference);
    }

    /**
     * Provide reference considered invalid (null) by this memory manager
     */
    @Override
    public long getInvalidReference() {
        return ReferenceCodecSeqExpand.INVALID_REFERENCE;
    }

    @Override
    public boolean isReferenceValid(long reference) {
        return rcd.isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceDeleted(long reference) {
        return rcd.isReferenceDeleted(reference);
    }

    @Override
    public boolean isReferenceValidAndNotDeleted(long reference) {
        return isReferenceValid(reference);
    }

    @Override
    public boolean isReferenceConsistent(long reference) {
        return rcd.isReferenceConsistent(reference);
    }

    @Override
    public Slice getEmptySlice() {
        return new SliceSeqExpand();
    }

    @Override public int getHeaderSize() {
        return 0;
    }
}

