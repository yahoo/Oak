package oak;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class WritableOakBufferImpl extends WritableOakBuffer {

    private Handle handle;
    private OakMemoryManager memoryManager;

    WritableOakBufferImpl(Handle handle, OakMemoryManager memoryManager) {
        this.handle = handle;
        this.memoryManager = memoryManager;
    }

    @Override
    public WritableOakBuffer position(int newPosition) {
        handle.position(newPosition);
        return this;
    }

    @Override
    public WritableOakBuffer mark() {
        handle.mark();
        return this;
    }

    @Override
    public WritableOakBuffer reset() {
        handle.reset();
        return this;
    }

    @Override
    public WritableOakBuffer clear() {
        handle.clear();
        return this;
    }

    @Override
    public WritableOakBuffer flip() {
        handle.flip();
        return this;
    }

    @Override
    public WritableOakBuffer rewind() {
        handle.rewind();
        return this;
    }

    @Override
    public byte get() {
        return handle.get();
    }

    @Override
    public WritableOakBuffer put(byte b) {
        while (true) {
            try {
                handle.put(b);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public int capacity() {
        return handle.capacity();
    }

    @Override
    public int position() {
        return handle.position();
    }

    @Override
    public int limit() {
        return handle.limit();
    }

    @Override
    public int remaining() {
        return handle.remaining();
    }

    @Override
    public boolean hasRemaining() {
        return handle.hasRemaining();
    }

    @Override
    public ByteBuffer getByteBuffer() { return handle.getByteBuffer(); }

    @Override
    public byte get(int index) {
        return handle.get(index);
    }

    @Override
    public WritableOakBuffer put(int index, byte b) {
        handle.put(index, b);
        return this;
    }

    @Override
    public WritableOakBuffer get(byte[] dst, int offset, int length) {
        handle.get(dst, offset, length);
        return this;
    }

    @Override
    public WritableOakBuffer put(byte[] src, int offset, int length) {
        while (true) {
            try {
                handle.put(src, offset, length);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public ByteOrder order() {
        return handle.order();
    }

    @Override
    public WritableOakBuffer order(ByteOrder bo) {
        handle.order();
        return this;
    }

    @Override
    public char getChar() {
        return handle.getChar();
    }

    @Override
    public WritableOakBuffer putChar(char value) {
        handle.putChar(value);
        return this;
    }

    @Override
    public char getChar(int index) {
        return handle.getChar(index);
    }

    @Override
    public WritableOakBuffer putChar(int index, char value) {
        while (true) {
            try {
                handle.putChar(index, value);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public short getShort() {
        return handle.getShort();
    }

    @Override
    public WritableOakBuffer putShort(short value) {
        handle.putShort(value);
        return this;
    }

    @Override
    public short getShort(int index) {
        return handle.getShort(index);
    }

    @Override
    public WritableOakBuffer putShort(int index, short value) {
        while (true) {
            try {
                handle.putShort(index, value);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public int getInt() {
        return handle.getInt();
    }

    @Override
    public WritableOakBuffer putInt(int value) {
        while (true) {
            try {
                handle.putInt(value);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public int getInt(int index) {
        return handle.getInt(index);
    }

    @Override
    public WritableOakBuffer putInt(int index, int value) {
        handle.putInt(index, value);
        return this;
    }

    @Override
    public long getLong() {
        return handle.getLong();
    }

    @Override
    public WritableOakBuffer putLong(long value) {
        while (true) {
            try {
                handle.putLong(value);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public long getLong(int index) {
        return handle.getLong(index);
    }

    @Override
    public WritableOakBuffer putLong(int index, long value) {
        handle.putLong(index, value);
        return this;
    }

    @Override
    public float getFloat() {
        return handle.getFloat();
    }

    @Override
    public WritableOakBuffer putFloat(float value) {
        while (true) {
            try {
                handle.putFloat(value);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public float getFloat(int index) {
        return handle.getFloat(index);
    }

    @Override
    public WritableOakBuffer putFloat(int index, float value) {
        handle.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble() {
        return handle.getDouble();
    }

    @Override
    public WritableOakBuffer putDouble(double value) {
        while (true) {
            try {
                handle.putDouble(value);
            } catch (BufferOverflowException e) {
                handle.increaseValueCapacity(memoryManager);
                continue;
            }
            break;
        }
        return this;
    }

    @Override
    public double getDouble(int index) {
        return handle.getDouble(index);
    }

    @Override
    public WritableOakBuffer putDouble(int index, double value) {
        handle.putDouble(index, value);
        return this;
    }

}
