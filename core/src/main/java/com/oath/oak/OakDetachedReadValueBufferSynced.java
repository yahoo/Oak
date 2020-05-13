/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.ConcurrentModificationException;
import java.util.function.Function;

/**
 * This class is used for when a detached access to the value is needed with synchronization:
 *  - zero-copy get operation
 *  - ValueIterator
 *  - EntryIterator (for values)
 *
 * It extends the non-synchronized version, and overrides the safeAccessToAttachedBuffer() method to perform
 * synchronization before any access to the data.
 */
class OakDetachedReadValueBufferSynced extends OakDetachedReadBuffer<ValueBuffer> {

    private static final int MAX_RETRIES = 1024;

    final KeyBuffer key;

    private final ValueUtils valueOperator;

    /**
     * In case of a search, this is the map we search in.
     */
    private final InternalOakMap<?, ?> internalOakMap;

    OakDetachedReadValueBufferSynced(KeyBuffer key, ValueBuffer value,
                                     ValueUtils valueOperator, InternalOakMap<?, ?> internalOakMap) {
        super(new ValueBuffer(value));
        this.key = new KeyBuffer(key);
        this.valueOperator = valueOperator;
        this.internalOakMap = internalOakMap;
    }

    @Override
    protected <T> T safeAccessToAttachedBuffer(Function<OakAttachedReadBuffer, T> transformer) {
        // Internal call. No input validation.

        start();
        try {
            return transformer.apply(buffer);
        } finally {
            end();
        }
    }

    private void start() {
        // Use a "for" loop to ensure maximal retries.
        for (int i = 0; i < MAX_RETRIES; i++) {
            ValueUtils.ValueResult res = valueOperator.lockRead(buffer);
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
        valueOperator.unlockRead(buffer);
    }

    /**
     * In case the version of the value pointed by {@code value} does not match its version, we assume
     * the value was moved and thus issue a search for this value. For that reason we have this field of the original
     * key of the original value. If the value was moved, using this key we are able to find it in Oak, or determine
     * it was deleted.
     */
    private void refreshValueReference() {
        boolean success = internalOakMap.refreshValuePosition(key, buffer);
        if (!success) {
            throw new ConcurrentModificationException();
        }
    }
}
