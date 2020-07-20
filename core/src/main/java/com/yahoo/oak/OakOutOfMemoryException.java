/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

public class OakOutOfMemoryException extends RuntimeException {
    public OakOutOfMemoryException() {
        super();
    }

    public OakOutOfMemoryException(String message) {
        super(message);
    }
}
