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

    private static final int LENGTH_OFFSET = LOCK_OFFSET+LOCK_SIZE;

    private static Unsafe unsafe = UnsafeUtils.unsafe;


    private static int getInt(Slice s, int headerOffsetInBytes) {
        return unsafe.getInt(s.getMetadataAddress() + headerOffsetInBytes);
    }

    private static void putInt(Slice s, int headerOffsetInBytes, int value) {
        unsafe.putInt(s.getMetadataAddress() + headerOffsetInBytes, value);
    }

    void setVersion(Slice s) {
        putInt(s, VERSION_OFFSET, s.getVersion());
    }

    int getVersion(Slice s) {
        return getInt(s, VERSION_OFFSET);
    }

    int getLockState(Slice s) {
        return getInt(s, LOCK_OFFSET);
    }

    void setLockState(Slice s, LockStates state) {
        putInt(s, LOCK_OFFSET, state.value);
    }

    int getLength(Slice s) {
        return getInt(s, LENGTH_OFFSET);
    }

    void setLength(Slice s, int length) {
        putInt(s, LENGTH_OFFSET, length);
    }

    private void initHeader(Slice s, LockStates state, int dataLength) {
        setVersion(s);
        setLockState(s, state);
        setLength(s, dataLength);
    }


    void initHeader(Slice s, int dataLength) {
        initHeader(s, LockStates.FREE, dataLength);
    }


    void initLockedHeader(Slice s, int dataLength) {
        initHeader(s, LockStates.LOCKED, dataLength);
    }

}
