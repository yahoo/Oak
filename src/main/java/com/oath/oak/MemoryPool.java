/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;

interface MemoryPool {

    Pair<Integer, ByteBuffer> allocate(int capacity); // TODO long capacity

    void free(int i, ByteBuffer bb);

    long allocated();

    void clean();

}
