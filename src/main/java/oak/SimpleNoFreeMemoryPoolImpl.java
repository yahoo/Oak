/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import javafx.util.Pair;
import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class SimpleNoFreeMemoryPoolImpl implements MemoryPool {

    final ByteBuffer largeBuffer; // TODO add more buffers
    final AtomicInteger allocated;
    final int capacity;

    SimpleNoFreeMemoryPoolImpl(long capacity) {
        assert capacity > 0;
        assert capacity <= Integer.MAX_VALUE;
        this.capacity = (int) capacity;
        allocated = new AtomicInteger();
        largeBuffer = ByteBuffer.allocateDirect(this.capacity);
    }

    @Override
    public Pair<Integer, ByteBuffer> allocate(int capacity) {
        int now = allocated.getAndAdd(capacity);
        if (now + capacity > this.capacity || capacity > Integer.MAX_VALUE - now) {
            throw new OakOutOfMemoryException();
        }
        ByteBuffer bb = largeBuffer.duplicate();
        bb.position(now);
        bb.limit(now + capacity);
        bb = bb.slice();
        assert bb.position() == 0;
        return new Pair<>(0, bb);
    }

    @Override
    public void free(int i, ByteBuffer bb) {
    }

    @Override
    public long allocated() {
        return allocated.get();
    }

    @Override
    public void clean() {
        Field cleanerField = null;
        try {
            cleanerField = largeBuffer.getClass().getDeclaredField("cleaner");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        cleanerField.setAccessible(true);
        Cleaner cleaner = null;
        try {
            cleaner = (Cleaner) cleanerField.get(largeBuffer);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        cleaner.clean();
    }
}