/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

public class KeyBuffer extends ScopedReadBuffer {

    // Should be used only when a new empty KeyBuffer needs to be created
    public KeyBuffer(Slice emptySlice) {
        super(emptySlice);
    }

    // Should be used only when a new KeyBuffer needs to be created as a copy of the given key
    public KeyBuffer(KeyBuffer key) {
        super(key);
    }

    // Should be used only when an existing KeyBuffer needs to be updated as a copy of the given key
    void copyFrom(KeyBuffer key) {
        if (key == this) {
            // No need to do anything if the input is this object
            return;
        }
        s.copyFrom(key.s);
    }
}
