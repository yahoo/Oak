/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.ConcurrentModificationException;

/**
 * This class is used for when an un-scoped access to the value is needed with synchronization:
 *  - zero-copy get operation
 *  - ValueIterator
 *  - EntryIterator (for values)
 * <p>
 * It extends the non-synchronized version, and overrides the transform() and safeAccessToScopedBuffer() methods to
 * perform synchronization before any access to the data.
 */
class UnscopedValueBufferSynced extends UnscopedBuffer<ValueBuffer> {

    private static final int MAX_RETRIES = 1024;

    final KeyBuffer key;

    /**
     * In case of a search, this is the map we search in.
     */
    private final InternalOakBasics<?, ?> internalOakBasics;

    UnscopedValueBufferSynced(KeyBuffer key, ValueBuffer value, InternalOakBasics<?, ?> internalOakBasics) {
        super(new ValueBuffer(value));
        this.key = new KeyBuffer(key);
        this.internalOakBasics = internalOakBasics;
    }

    @Override
    public <T> T transform(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        start();
        try {
            return transformer.apply(internalScopedReadBuffer);
        } finally {
            end();
        }
    }

    @Override
    protected <R> R safeAccessToScopedBuffer(Getter<R> getter, int index) {
        // Internal call. No input validation.

        start();
        try {
            return getter.get(internalScopedReadBuffer, index);
        } finally {
            end();
        }
    }

    private void start() {
        // Use a "for" loop to ensure maximal retries.
        for (int i = 0; i < MAX_RETRIES; i++) {
            ValueUtils.ValueResult res = internalScopedReadBuffer.s.preRead();
            switch (res) {
                case TRUE:
                    return;
                case FALSE:
                    throw new ConcurrentModificationException();
                case RETRY:
                    refreshValueReference();
                    break;
            }
        }

        throw new RuntimeException("Op failed: reached retry limit (1024).");
    }

    private void end() {
        internalScopedReadBuffer.s.postRead();
    }

    /**
     * In case the version of the value pointed by {@code value} does not match its version, we assume
     * the value was moved and thus issue a search for this value. For that reason we have this field of the original
     * key of the original value. If the value was moved, using this key we are able to find it in Oak, or determine
     * it was deleted.
     */
    private void refreshValueReference() {
        boolean success = internalOakBasics.refreshValuePosition(key, internalScopedReadBuffer);
        if (!success) {
            throw new ConcurrentModificationException();
        }
    }
}
