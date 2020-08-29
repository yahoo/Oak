/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

public class ValueBuffer extends ScopedReadBuffer {

    public ValueBuffer(int headerSize) {
        super(headerSize);
    }

    public ValueBuffer(ValueUtils valueOperator) {
        super(valueOperator.getHeaderSize());
    }

    public ValueBuffer(ValueBuffer value) {
        super(value);
    }

    @Override
    void invalidate() {
        super.invalidate();
    }

    void copyFrom(ValueBuffer alloc) {
        if (alloc == this) {
            // No need to do anything if the input is this object
            return;
        }
        super.copyFrom(alloc);
    }
}
