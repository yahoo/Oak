/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * An interface to be supported by keys and values provided for Oak's mapping
 */
public interface OakSerializer<T> {

    // serializes the object
    void serialize(T object, OakScopedWriteBuffer targetBuffer);

    // deserializes the given Oak buffer
    T deserialize(OakScopedReadBuffer byteBuffer);

    // returns the number of bytes needed for serializing the given object
    int calculateSize(T object);
}
