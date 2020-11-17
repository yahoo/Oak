/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

public class ValueBuffer extends ScopedReadBuffer {

    // Should be used only when a new empty ValueBuffer needs to be created
    public ValueBuffer(Slice emptySlice) {
        super(emptySlice);
    }

    // Should be used only when a new ValueBuffer needs to be created as a copy of the given value
    public ValueBuffer(ValueBuffer value) {
        super(value);
    }

    // Should be used only when an existing ValueBuffer needs to be updated as a copy of the given value
    void copyFrom(ValueBuffer value) {
        if (value == this) {
            // No need to do anything if the input is this object
            return;
        }
        s.copyFrom(value.s);
    }
}
