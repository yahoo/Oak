/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * This Exception is thrown when using the Nova MemoryManager, such exception indicates that we are trying
 * to access some off heap memory location that was deleted concurrently to the access.
 */
public class DeletedMemoryAccessException extends Exception {
    public DeletedMemoryAccessException() {
        super();
    }

    public DeletedMemoryAccessException(String message) {
        super(message);
    }
}
