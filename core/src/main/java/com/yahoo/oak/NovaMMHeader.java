/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.ValueUtils.ValueResult;

import java.util.concurrent.atomic.AtomicLongArray;


class NovaMMHeader {

    /*
    * Long NovaMMHeader: int version + long length
    * 0...  ...21 | 32...                  ...63
    *  version    |  length
    *
    * The length (Mostly in integer size) of the data is also held just following the header (since the block and
    * offset are 42 bits at most the length can be 42 bits which we allow here!)
    * The length is set once upon header allocation and later can only be read. */

    private static final ReferenceCodecNovaHeader RC = new ReferenceCodecNovaHeader();
    
    long getOffHeapHeader(long headerAddress) {
        return DirectUtils.UNSAFE.getLong(headerAddress);
    }

    public int getDataLength(long headerAddress) {
        long offHeapHeader = getOffHeapHeader(headerAddress);
        return RC.getSecond(offHeapHeader); //assuming length is int
    }

    public void initHeader(long headerAddress, int dataLength, int version) {
        //TODO add assert for length < 2^32?
        long header = RC.encode(version, dataLength);
        DirectUtils.UNSAFE.putLong(headerAddress, header);
    }


    
    private static boolean atomicallySetDeleted(long headerAddress, long offHeapMeta) {
        // this CAS replaces the old offheap version with new one that has 1 as the first bit to indicate deletion
        return DirectUtils.UNSAFE.compareAndSwapLong(null, headerAddress, offHeapMeta, offHeapMeta | 1);
    }
    
    
    ValueUtils.ValueResult preRead(final int onHeapVersion, long headerAddress) {
        long offHeapHeader = getOffHeapHeader(headerAddress);
        if (RC.isReferenceDeleted(offHeapHeader)) {
            throw new DeletedMemoryAccessException();
        }
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult postRead(final int onHeapVersion, long headerAddress) {
        DirectUtils.UNSAFE.loadFence();
        long offHeapHeader = getOffHeapHeader(headerAddress);
        int offHeapVersion = RC.getFirst(offHeapHeader);
        if (onHeapVersion != offHeapVersion) {
            throw new DeletedMemoryAccessException();
        }
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult preWrite(final int onHeapVersion, final long tapEntry,
               long headerAddress, AtomicLongArray tap) {
        long offHeapHeader = getOffHeapHeader(headerAddress);
        if (RC.isReferenceDeleted(offHeapHeader)) {
            return ValueUtils.ValueResult.RETRY;
        }
        NovaMemoryManager.setTap(tap, tapEntry,
                (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS);
        DirectUtils.UNSAFE.fullFence();
        int offHeapVersion = RC.getFirst(offHeapHeader);
        if (onHeapVersion != offHeapVersion) {
            NovaMemoryManager.resetTap(tap, 
                    (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS);
            return ValueResult.FALSE;
        }
        
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult postWrite(final int onHeapVersion, long headerAddress, AtomicLongArray tap) {
        DirectUtils.UNSAFE.storeFence();
        NovaMemoryManager.resetTap(tap, 
                (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS);
        return ValueUtils.ValueResult.TRUE;
        //can be replaced with always true without unsetting the tap
    }

    ValueUtils.ValueResult logicalDelete(final int onHeapVersion , long headerAddress) {
        assert onHeapVersion > ReferenceCodecSyncRecycle.INVALID_VERSION;
        long offHeapHeader = -1;
        do {
            offHeapHeader = getOffHeapHeader(headerAddress);
            int oldVersion = RC.getFirst(offHeapHeader);
            if (oldVersion  != onHeapVersion ) {
                return ValueUtils.ValueResult.RETRY; //TODO retry?
            }
            if (RC.isReferenceDeleted(offHeapHeader)) {
                return ValueUtils.ValueResult.FALSE;
            }
        } while (!atomicallySetDeleted(headerAddress,  offHeapHeader));
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult isLogicallyDeleted(final int onHeapVersion, long headerAddress) {
        long offHeapHeader = getOffHeapHeader(headerAddress);
        int oldVersion = RC.getFirst(offHeapHeader);
        if (RC.isReferenceDeleted(offHeapHeader)) {
            return ValueUtils.ValueResult.TRUE;
        }
        if (oldVersion != onHeapVersion) {
            return ValueUtils.ValueResult.RETRY;
        }
        return ValueUtils.ValueResult.FALSE;
    }

    void markAsMoved(long headerAddress) {
        throw new IllegalAccessError("not in use for Nova");
    }
}
