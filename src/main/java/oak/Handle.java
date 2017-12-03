package oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

abstract class Handle extends WritableOakBuffer {

    final ReentrantReadWriteLock.ReadLock readLock;
    final ReentrantReadWriteLock.WriteLock writeLock;
    ByteBuffer value;
    final AtomicBoolean deleted;

    Handle(ByteBuffer value) {
        this.value = value;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
        this.deleted = new AtomicBoolean(false);
    }

    abstract void increaseValueCapacity(OakMemoryManager memoryManager);

    abstract void setValue(ByteBuffer value, int i);

    boolean isDeleted() {
        return deleted.get();
    }

    abstract boolean remove(OakMemoryManager memoryManager);

    abstract void put(ByteBuffer value, OakMemoryManager memoryManager);

    void compute(Consumer<WritableOakBuffer> function, OakMemoryManager memoryManager) {
        writeLock.lock();
        if (isDeleted()) {
            writeLock.unlock();
            return;
        }
        // TODO: invalidate oak buffer after accept
        try {
            function.accept(new WritableOakBufferImpl(this, memoryManager));
        } finally {
            value.rewind(); // TODO rewind?
            writeLock.unlock();
        }
    }

    @Override
    WritableOakBuffer position(int newPosition) {
        assert writeLock.isHeldByCurrentThread();
        value.position(newPosition);
        return this;
    }

    @Override
    WritableOakBuffer mark() {
        assert writeLock.isHeldByCurrentThread();
        value.mark();
        return this;
    }

    @Override
    WritableOakBuffer reset() {
        assert writeLock.isHeldByCurrentThread();
        value.reset();
        return this;
    }

    @Override
    WritableOakBuffer clear() {
        assert writeLock.isHeldByCurrentThread();
        value.clear();
        return this;
    }

    @Override
    WritableOakBuffer flip() {
        assert writeLock.isHeldByCurrentThread();
        value.flip();
        return this;
    }

    @Override
    WritableOakBuffer rewind() {
        assert writeLock.isHeldByCurrentThread();
        value.rewind();
        return this;
    }

    @Override
    public byte get() {
        return value.get();
    }

    @Override
    public WritableOakBuffer put(byte b) {
        assert writeLock.isHeldByCurrentThread();
        value.put(b);
        return this;
    }

    @Override
    int capacity() {
        return value.capacity();
    }

    int capacityLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int capacity;
        try {
            capacity = value.capacity();
        } finally {
            readLock.unlock();
        }
        return capacity;
    }

    @Override
    int position() {
        return value.position();
    }

    int positionLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int position;
        try {
            position = value.position();
        } finally {
            readLock.unlock();
        }
        return position;
    }

    @Override
    int limit() {
        return value.limit();
    }

    int limitLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int limit;
        try {
            limit = value.limit();
        } finally {
            readLock.unlock();
        }
        return limit;
    }

    @Override
    int remaining() {
        return value.remaining();
    }

    int remainingLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int remaining;
        try {
            remaining = value.remaining();
        } finally {
            readLock.unlock();
        }
        return remaining;
    }

    @Override
    boolean hasRemaining() {
        return value.hasRemaining();
    }

    boolean hasRemainingLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        boolean hasRemaining;
        try {
            hasRemaining = value.hasRemaining();
        } finally {
            readLock.unlock();
        }
        return hasRemaining;
    }

    @Override
    public byte get(int index) {
        return value.get(index);
    }

    byte getLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        byte b;
        try {
            b = get(index);
        } finally {
            readLock.unlock();
        }
        return b;
    }

    @Override
    WritableOakBuffer put(int index, byte b) {
        assert writeLock.isHeldByCurrentThread();
        value.put(index, b);
        return this;
    }

    @Override
    public WritableOakBuffer get(byte[] dst, int offset, int length) {
        value.get(dst, offset, length);
        return this;
    }

    @Override
    WritableOakBuffer put(byte[] src, int offset, int length) {
        assert writeLock.isHeldByCurrentThread();
        value.put(src, offset, length);
        return this;
    }

    @Override
    ByteOrder order() {
        return value.order();
    }

    ByteOrder orderLock() {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        ByteOrder order;
        try {
            order = order();
        } finally {
            readLock.unlock();
        }
        return order;
    }

    @Override
    WritableOakBuffer order(ByteOrder bo) {
        value.order(bo);
        return this;
    }

    @Override
    char getChar() {
        return value.getChar();
    }

    @Override
    WritableOakBuffer putChar(char value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putChar(value);
        return this;
    }

    @Override
    char getChar(int index) {
        return value.getChar(index);
    }

    char getCharLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        char c;
        try {
            c = getChar(index);
        } finally {
            readLock.unlock();
        }
        return c;
    }

    @Override
    WritableOakBuffer putChar(int index, char value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putChar(index, value);
        return this;
    }

    @Override
    short getShort() {
        return value.getShort();
    }

    @Override
    WritableOakBuffer putShort(short value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putShort(value);
        return this;
    }

    @Override
    short getShort(int index) {
        return value.getShort(index);
    }

    short getShortLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        short s;
        try {
            s = getShort(index);
        } finally {
            readLock.unlock();
        }
        return s;
    }

    @Override
    WritableOakBuffer putShort(int index, short value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putShort(index, value);
        return this;
    }

    @Override
    int getInt() {
        return value.getInt();
    }

    @Override
    WritableOakBuffer putInt(int value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putInt(value);
        return this;
    }

    @Override
    int getInt(int index) {
        return value.getInt(index);
    }

    int getIntLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        int j;
        try {
            j = getInt(index);
        } finally {
            readLock.unlock();
        }
        return j;
    }

    @Override
    WritableOakBuffer putInt(int index, int value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putInt(index, value);
        return this;
    }

    @Override
    long getLong() {
        return value.getLong();
    }

    @Override
    WritableOakBuffer putLong(long value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putLong(value);
        return this;
    }

    @Override
    long getLong(int index) {
        return value.getLong(index);
    }

    long getLongLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        long l;
        try {
            l = getLong(index);
        } finally {
            readLock.unlock();
        }
        return l;
    }

    @Override
    WritableOakBuffer putLong(int index, long value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putLong(index, value);
        return this;
    }

    @Override
    float getFloat() {
        return value.getFloat();
    }

    @Override
    WritableOakBuffer putFloat(float value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putFloat(value);
        return this;
    }

    @Override
    float getFloat(int index) {
        return value.getFloat(index);
    }

    float getFloatLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        float f;
        try {
            f = getFloat(index);
        } finally {
            readLock.unlock();
        }
        return f;
    }

    @Override
    WritableOakBuffer putFloat(int index, float value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putFloat(index, value);
        return this;
    }

    @Override
    double getDouble() {
        return value.getDouble();
    }

    @Override
    WritableOakBuffer putDouble(double value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putDouble(value);
        return this;
    }

    @Override
    double getDouble(int index) {
        return value.getDouble(index);
    }

    double getDoubleLock(int index) {
        readLock.lock();
        if (isDeleted()) {
            readLock.unlock();
            throw new NullPointerException();
        }
        double d;
        try {
            d = getDouble(index);
        } finally {
            readLock.unlock();
        }
        return d;
    }

    @Override
    WritableOakBuffer putDouble(int index, double value) {
        assert writeLock.isHeldByCurrentThread();
        this.value.putDouble(index, value);
        return this;
    }

}
