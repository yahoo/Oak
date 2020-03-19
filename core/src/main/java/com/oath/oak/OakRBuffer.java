/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

/**
 * A similar to read-only ByteBuffer interface that allows internal Oak data read access
 * <p>
 * Pay attention! There is no need to wrap each OakRBuffer interface implementation
 * with attach/detach thread, because OakRKeyBufferImpl is used only within keyIterator, which
 * has attach/detach thread on its own. For the same reason here is no transform() method.
 */
public interface OakRBuffer extends OakReadBuffer {

    /**
     * Perform a transformation on the inner ByteBuffer atomically.
     *
     * @param transformer The function to apply on the ByteBuffer
     * @return The return value of the transform
     */
    <T> T transform(OakTransformer<T> transformer);

}
