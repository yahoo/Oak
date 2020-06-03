/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * This buffer may represent either a key or a value for read access.
 * It mimic the standard interface of Java's ByteBuffer, for example, int getInt(int index), char getChar(int index),
 * capacity(), etc.
 * <p>
 * It is attached to the scope of the callback method it were first introduced to the user.
 * The behaviour of this buffer outside their attached scope is undefined.
 * Such callback method might be the application's serializer and comparator, or a lambda function that can
 * read/store/update the data.
 * This access reduces unnecessary copies and deserialization of the underlying data.
 * In their intended context, the user does not need to worry about concurrent accesses and memory management.
 * Using these buffers outside their intended context may yield unpredicted results, e.g., reading non-consistent data
 * and/or irrelevant data.
 */
public interface OakScopedReadBuffer extends OakBuffer {
}
