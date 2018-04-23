package oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

class HandleOffHeapImpl extends Handle {

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
    void put(ByteBuffer value, OakMemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return;
        }
        int pos = value.position();
        int rem = value.remaining();
        if (this.value.remaining() >= rem) { // try to reuse old space
            for (int j = 0; j < rem; j++) {
                this.value.put(j, value.get(pos + j));
            }
        } else {
            memoryManager.release(this.i, this.value);
            Pair<Integer, ByteBuffer> pair = memoryManager.allocate(rem);
            this.i = pair.getKey();
            this.value = pair.getValue();
            for (int j = 0; j < rem; j++) {
                this.value.put(j, value.get(pos + j));
            }
        }
        writeLock.unlock();
    }

    @Override
    void put(Consumer<ByteBuffer> valueCreator, int capacity, OakMemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return;
        }
        if (this.value.remaining() < capacity) { // try to reuse old space
            memoryManager.release(this.i, this.value);
            Pair<Integer, ByteBuffer> pair = memoryManager.allocate(capacity);
            this.i = pair.getKey();
            this.value = pair.getValue();
        }
        valueCreator.accept(this.value);
        writeLock.unlock();
    }

}
