/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

class HandleOffHeapImpl<V> extends Handle<V> {

    private int i;

    HandleOffHeapImpl(ByteBuffer value, int i) {
        super(value);
        this.i = i;
    }

    @Override
    void increaseValueCapacity(OakMemoryManager memoryManager) {
        assert writeLock.isHeldByCurrentThread();
        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(value.capacity() * 2);
        ByteBuffer newValue = pair.getValue();
        for (int j = 0; j < value.limit(); j++) {
            newValue.put(j, value.get(j));
        }
        newValue.position(value.position());
        memoryManager.release(this.i, this.value);
        value = newValue;
        i = pair.getKey();
    }

    @Override
    void setValue(ByteBuffer value, int i) {
        this.i = i;
        this.value = value;
    }

    @Override
    boolean remove(OakMemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        deleted.set(true);
        writeLock.unlock();
        memoryManager.release(i, value);
        return true;
    }

    @Override
    void put(V newVal, Serializer<V> serializer, SizeCalculator<V> sizeCalculator, OakMemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return;
        }
        int capacity = sizeCalculator.calculateSize(newVal);
        if (this.value.remaining() < capacity) { // try to reuse old space
            memoryManager.release(this.i, this.value);
            Pair<Integer, ByteBuffer> pair = memoryManager.allocate(capacity);
            this.i = pair.getKey();
            this.value = pair.getValue();
        }
        serializer.serialize(newVal, this.value);
        writeLock.unlock();
    }

}
