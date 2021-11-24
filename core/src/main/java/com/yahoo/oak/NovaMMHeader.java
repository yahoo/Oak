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
    * Long NovaMMHeader: int version + int lock
    * 0...  ...21 | 32...                  ...63
    *  version    |  length
    *
    * The length (Mostly in integer size) of the data is also held just following the header (since the block and
    * offset are 42 bits at most the length can be 42 bits which we allow here!)
    * The length is set once upon header allocation and later can only be read. */

    private static final int VERSION_SIZE = 22;  //block offset get 42 bits on heap-(including one bit for deleted)
   
    private static final int OFFHEAP_VERSION_MASK = 0x3F_FF_FF;


    int getOffHeapVersion(long headerAddress) {
        return (int) (UnsafeUtils.UNSAFE.getLong(headerAddress) & OFFHEAP_VERSION_MASK);
    }

    int getDataLength(long headerAddress) {
        return (int) (UnsafeUtils.UNSAFE.getLong(headerAddress) >>> VERSION_SIZE); //TODO length can be long??
    }
    
    long getOffHeapMetaData(long headerAddress) {
        return UnsafeUtils.UNSAFE.getLong(headerAddress);
    }

    private void setDataLength(long headerAddress, int length) {
        throw new IllegalAccessError();
    }

    void initHeader(long headerAddress, int dataLength, int version) {
        long sliceHeader    = (long) (dataLength) << VERSION_SIZE;
        int  newVer         = version & 0x3FFFFF; //VERSION_SIZE;
        long header         = sliceHeader | newVer ;
        UnsafeUtils.UNSAFE.putLong(headerAddress, header);
    }

    public  void setTap(AtomicLongArray tap, long ref, int idx) {
        int i = idx % ThreadIndexCalculator.MAX_THREADS;
        tap.set(NovaMemoryManager.CACHE_PADDING * (i + 1) + NovaMemoryManager.IDENTRY, idx);
        tap.set(NovaMemoryManager.CACHE_PADDING * (i + 1) + NovaMemoryManager.REFENTRY, ref);
    }
        
    public  void unsetTap(AtomicLongArray tap, int idx) {
        int i = idx % ThreadIndexCalculator.MAX_THREADS;
        tap.set(NovaMemoryManager.CACHE_PADDING * (i + 1) + NovaMemoryManager.IDENTRY, -1);
    }


    /*---------------- Locking Implementation ----------------*/

    private static boolean cas(long headerAddress, int expectedLock, int newLock, int version) {
        // Since the writing is done directly to the memory, the endianness of the memory is important here.
        // Therefore, we make sure that the values are read and written correctly.
        long expected = UnsafeUtils.intsToLong(version, expectedLock);
        long value = UnsafeUtils.intsToLong(version, newLock);
        return UnsafeUtils.UNSAFE.compareAndSwapLong(null, headerAddress, expected, value);
    }

    
    private static boolean cas(long headerAddress, long offHeapMeta) {
        // Since the writing is done directly to the memory, the endianness of the memory is important here.
        // Therefore, we make sure that the values are read and written correctly.
        return UnsafeUtils.UNSAFE.compareAndSwapLong(null, headerAddress, offHeapMeta, offHeapMeta | 1);
    }
    
    
    ValueUtils.ValueResult lockRead(final int onHeapVersion, long headerAddress) {
        if (onHeapVersion % 2 == 1) {
            throw new DeletedMemoryAccessException();
        }
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult unlockRead(final int onHeapVersion, long headerAddress) {
        UnsafeUtils.UNSAFE.loadFence();
        if (! (onHeapVersion == (int) (UnsafeUtils.UNSAFE.getLong(headerAddress) & 0x1FFFFF))) {
            throw new DeletedMemoryAccessException();
        }
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult lockWrite(final int onHeapVersion, final int blockID, final int offset,
               long headerAddress, AtomicLongArray tap) {
        if (onHeapVersion % 2 == 1) {
            return ValueUtils.ValueResult.RETRY;
        }
        long ref = blockID << 20 | offset;
        setTap(tap, ref, (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS);
        UnsafeUtils.UNSAFE.fullFence();
        
        if (! (onHeapVersion == (int) (UnsafeUtils.UNSAFE.getLong(headerAddress) & 0x1FFFFF))) {
            unsetTap(tap, (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS);
            return ValueResult.FALSE;
        }
        
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult unlockWrite(final int onHeapVersion, long headerAddress, AtomicLongArray tap) {
        UnsafeUtils.UNSAFE.storeFence();
        unsetTap(tap, (int) Thread.currentThread().getId() % ThreadIndexCalculator.MAX_THREADS);
        return ValueUtils.ValueResult.TRUE;
        //can be replaced with always true without unsetting the tap
    }

    ValueUtils.ValueResult logicalDelete(final int onHeapVersion , long headerAddress) {
        assert onHeapVersion > ReferenceCodecSyncRecycle.INVALID_VERSION;
        long lengthVer = -1;
        do {
            lengthVer = getOffHeapMetaData(headerAddress);
            int oldVersion = (int) lengthVer & 0x1F_FF_FF;
            if (oldVersion >> 1 != onHeapVersion >> 1) {
                return ValueUtils.ValueResult.RETRY;
            }
            if (oldVersion % 2 == 1) {
                return ValueUtils.ValueResult.FALSE;
            }
        } while (!cas(headerAddress,  lengthVer));
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult isLogicallyDeleted(final int onHeapVersion, long headerAddress) {
        int oldVersion = getOffHeapVersion(headerAddress) & 0x1F_FF_FF;
        if (oldVersion % 2 == 1) {
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
