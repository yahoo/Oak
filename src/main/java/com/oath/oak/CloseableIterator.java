/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.Iterator;

/**
 * A concurrent map iterator interface that needs to be closed by the end of usage.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
    @Override
    void close();
}
