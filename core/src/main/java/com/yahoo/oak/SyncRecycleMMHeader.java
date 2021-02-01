/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Unsafe;

class SyncRecycleMMHeader {

    /*
    * Long SyncRecycleMMHeader: int version + int lock
    * 0...  ...31 | 32...                  ...61| 62 63
    *  version    |   lock: current_readers#    | lock state
    *
    * lock state: 0x00 - FREE, 0x01 - LOCKED, 0x10 - DELETED, 0x11 - MOVED
    *
    * The length (in integer size) of the data is also held just following the header (in long size)
    * The length is set once upon header allocation and later can only be read. */

    enum LockStates {
        FREE(0), LOCKED(1), DELETED(2), MOVED(3);

        public final int value;

        LockStates(int value) {
            this.value = value;
        }
    }

    private static final int VERSION_SIZE = 4;
    private static final int VERSION_OFFSET = 0;

    private static final int LOCK_SIZE = 4;
    private static final int LOCK_OFFSET = VERSION_SIZE;

    private static final int LOCK_STATE_MASK = 0x3;
    private static final int LOCK_STATE_SHIFT = 2;

    private static final int LENGTH_OFFSET = VERSION_SIZE+LOCK_SIZE;

    private static Unsafe unsafe = UnsafeUtils.unsafe;


    private static int getInt(long headerAddress, int intOffsetInBytes) {
        return unsafe.getInt(headerAddress + intOffsetInBytes);
    }

    private static void putInt(long headerAddress, int intOffsetInBytes, int value) {
        unsafe.putInt(headerAddress + intOffsetInBytes, value);
    }

    private void setOffHeapVersion(long headerAddress, int offHeapVersion) {
        putInt(headerAddress, VERSION_OFFSET, offHeapVersion);
    }

    int getOffHeapVersion(long headerAddress) {
        return getInt(headerAddress, VERSION_OFFSET);
    }

    private int getLockState(long headerAddress) {
        return getInt(headerAddress, LOCK_OFFSET);
    }

    private void setLockState(long headerAddress, LockStates state) {
        putInt(headerAddress, LOCK_OFFSET, state.value);
    }

    int getDataLength(long headerAddress) {
        return getInt(headerAddress, LENGTH_OFFSET);
    }

    private void setDataLength(long headerAddress, int length) {
        putInt(headerAddress, LENGTH_OFFSET, length);
    }

    private void initHeader(long headerAddress, LockStates state, int dataLength, int version) {
        setOffHeapVersion(headerAddress, version);
        setLockState(headerAddress, state);
        setDataLength(headerAddress, dataLength);
    }

    void initFreeHeader(long headerAddress, int dataLength, int version) {
        initHeader(headerAddress, LockStates.FREE, dataLength, version);
    }

    void initLockedHeader(long headerAddress, int dataLength, int version) {
        initHeader(headerAddress, LockStates.LOCKED, dataLength, version);
    }

    /*---------------- Locking Implementation ----------------*/

    private static boolean cas(long headerAddress, int expectedLock, int newLock, int version) {
        // Since the writing is done directly to the memory, the endianness of the memory is important here.
        // Therefore, we make sure that the values are read and written correctly.
        long expected = UnsafeUtils.intsToLong(version, expectedLock);
        long value = UnsafeUtils.intsToLong(version, newLock);
        return unsafe.compareAndSwapLong(null, headerAddress, expected, value);
    }

    ValueUtils.ValueResult lockRead(final int onHeapVersion, long headerAddress) {
        int lockState;
        assert onHeapVersion > ReferenceCodecSyncRecycle.INVALID_VERSION
            : "In locking for read the version was: " + onHeapVersion;
        do {
            int offHeapVersion = getOffHeapVersion(headerAddress);
            if (offHeapVersion != onHeapVersion) {
                return ValueUtils.ValueResult.RETRY;
            }
            lockState = getLockState(headerAddress);
            if (offHeapVersion != getOffHeapVersion(headerAddress)) {
                return ValueUtils.ValueResult.RETRY;
            }
            if (lockState == LockStates.DELETED.value) {
                return ValueUtils.ValueResult.FALSE;
            }
            if (lockState == LockStates.MOVED.value) {
                return ValueUtils.ValueResult.RETRY;
            }
            lockState &= ~LOCK_STATE_MASK;
        } while (!cas(headerAddress, lockState, lockState + (1 << LOCK_STATE_SHIFT), onHeapVersion));
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult unlockRead(final int onHeapVersion, long headerAddress) {
        int lockState;
        assert onHeapVersion > ReferenceCodecSyncRecycle.INVALID_VERSION;
        do {
            lockState = getLockState(headerAddress);
            assert lockState > LockStates.MOVED.value;
            lockState &= ~LOCK_STATE_MASK;
        } while (!cas(headerAddress, lockState, lockState - (1 << LOCK_STATE_SHIFT), onHeapVersion));
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult lockWrite(final int onHeapVersion, long headerAddress) {
        assert onHeapVersion > ReferenceCodecSyncRecycle.INVALID_VERSION;
        do {
            int oldVersion = getOffHeapVersion(headerAddress);
            if (oldVersion != onHeapVersion) {
                return ValueUtils.ValueResult.RETRY;
            }
            int lockState = getLockState(headerAddress);
            if (oldVersion != getOffHeapVersion(headerAddress)) {
                return ValueUtils.ValueResult.RETRY;
            }
            if (lockState == LockStates.DELETED.value) {
                return ValueUtils.ValueResult.FALSE;
            }
            if (lockState == LockStates.MOVED.value) {
                return ValueUtils.ValueResult.RETRY;
            }
        } while (!cas(headerAddress, LockStates.FREE.value, LockStates.LOCKED.value, onHeapVersion));
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult unlockWrite(final int onHeapVersion, long headerAddress) {
        int lockState = getLockState(headerAddress);
        assert (lockState == LockStates.LOCKED.value)
            && (onHeapVersion == getOffHeapVersion(headerAddress));
        // use CAS and not just write so potential waiting reads can proceed immediately
        cas(headerAddress, lockState, LockStates.FREE.value, onHeapVersion);
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult logicalDelete(final int onHeapVersion, long headerAddress) {
        assert onHeapVersion > ReferenceCodecSyncRecycle.INVALID_VERSION;
        do {
            int oldVersion = getOffHeapVersion(headerAddress);
            if (oldVersion != onHeapVersion) {
                return ValueUtils.ValueResult.RETRY;
            }
            int lockState = getLockState(headerAddress);
            if (oldVersion != getOffHeapVersion(headerAddress)) {
                return ValueUtils.ValueResult.RETRY;
            }
            if (lockState == LockStates.DELETED.value) {
                return ValueUtils.ValueResult.FALSE;
            }
            if (lockState == LockStates.MOVED.value) {
                return ValueUtils.ValueResult.RETRY;
            }
        } while (!cas(headerAddress, LockStates.FREE.value, LockStates.DELETED.value, onHeapVersion));
        return ValueUtils.ValueResult.TRUE;
    }

    ValueUtils.ValueResult isLogicallyDeleted(final int onHeapVersion, long headerAddress) {
        int oldVersion = getOffHeapVersion(headerAddress);
        if (oldVersion != onHeapVersion) {
            return ValueUtils.ValueResult.RETRY;
        }
        int lockState = getLockState(headerAddress);
        if (oldVersion != getOffHeapVersion(headerAddress)) {
            return ValueUtils.ValueResult.RETRY;
        }
        if (lockState == LockStates.MOVED.value) {
            return ValueUtils.ValueResult.RETRY;
        }
        if (lockState == LockStates.DELETED.value) {
            return ValueUtils.ValueResult.TRUE;
        }
        return ValueUtils.ValueResult.FALSE;
    }

    void markAsMoved(long headerAddress) {
        assert getLockState(headerAddress) == LockStates.LOCKED.value;
        setLockState(headerAddress, LockStates.MOVED);
    }

    void markAsDeleted(long headerAddress) {
        assert getLockState(headerAddress) == LockStates.LOCKED.value;
        setLockState(headerAddress, LockStates.DELETED);
    }
}
