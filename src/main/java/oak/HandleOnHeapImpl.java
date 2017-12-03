package oak;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

class HandleOnHeapImpl extends Handle {

    HandleOnHeapImpl(ByteBuffer value) {
        super(value);
    }

    @Override
    void increaseValueCapacity(OakMemoryManager memoryManager) {
        assert memoryManager == null;
        assert writeLock.isHeldByCurrentThread();
        ByteBuffer newValue = ByteBuffer.allocate(value.capacity() * 2);
        newValue.put(value.array());
        newValue.position(value.position());
        value = newValue;
    }

    @Override
    void setValue(ByteBuffer value, int i) {
        this.value = value;
        // i is ignored, used only for off heap
    }

    @Override
    boolean remove(OakMemoryManager memoryManager) {
        assert memoryManager == null;
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return false;
        }
        deleted.set(true);
        writeLock.unlock();
        return true;
    }

    @Override
    void put(ByteBuffer value, OakMemoryManager memoryManager) {
        assert memoryManager == null;
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
            byte[] valueArray = new byte[rem];
            System.arraycopy(value.array(), value.position(), valueArray, 0, value.remaining());
            this.value = ByteBuffer.wrap(valueArray);
        }
        writeLock.unlock();
    }

    @Override
    void compute(Consumer<WritableOakBuffer> function, OakMemoryManager memoryManager) {
        assert memoryManager == null;
        super.compute(function, memoryManager);
    }

}
