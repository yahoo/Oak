package com.oath.oak.synchrobench.maps;


import java.nio.ByteBuffer;

public class MyBuffer implements Comparable<MyBuffer>{

    public final int capacity;
    public final ByteBuffer buffer;

    public MyBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = ByteBuffer.allocate(capacity);
    }

    @Override // for ConcurrentSkipListMap
    public int compareTo(MyBuffer o) {
        return compareBuffers(this, o);
    }

    public static void serialize(MyBuffer inputBuffer, ByteBuffer targetBuffer) {
        int targetPos = targetBuffer.position();
        serialize(inputBuffer, targetBuffer, targetPos);
    }

    public static void serialize(MyBuffer inputBuffer, ByteBuffer targetBuffer, int targetPos) {
        // write the capacity in the beginning of the buffer
        targetBuffer.putInt(targetPos, inputBuffer.capacity);
        targetPos += Integer.BYTES;
        for (int i = 0; i < inputBuffer.capacity; i += Integer.BYTES) {
            targetBuffer.putInt(targetPos + i, inputBuffer.buffer.getInt(i));
        }
    }

    public static MyBuffer deserialize(ByteBuffer inputBuffer) {
        int inputPos = inputBuffer.position();
        return deserialize(inputBuffer, inputPos);
    }

    public static MyBuffer deserialize(ByteBuffer inputBuffer, int inputPos) {
        int cap = inputBuffer.getInt(inputPos);
        inputPos += Integer.BYTES;
        MyBuffer ret = new MyBuffer(cap);
        for (int i = 0; i < cap; i += Integer.BYTES) {
            ret.buffer.putInt(i, inputBuffer.getInt(inputPos + i));
        }
        return ret;
    }

    public static int compareBuffers(ByteBuffer buffer1, int base1, int len1, ByteBuffer buffer2, int base2, int len2) {
        int n = Math.min(len1, len2);
        for (int i = 0; i < n; i += Integer.BYTES) {
            int cmp = Integer.compare(buffer1.getInt(base1 + i), buffer2.getInt(base2 + i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return (len1 - len2);
    }

    public static int compareBuffers(ByteBuffer buffer1, ByteBuffer buffer2) {
        return compareBuffers(buffer1, buffer1.position(), buffer1.capacity(),
            buffer2, buffer2.position(), buffer2.capacity());
    }

    public static int compareBuffers(MyBuffer buffer1, MyBuffer buffer2) {
        return compareBuffers(buffer1.buffer, buffer2.buffer);
    }

    public static int compareBuffers(MyBuffer key1, ByteBuffer key2) {
        return compareBuffers(key1.buffer, key1.buffer.position(), key1.buffer.capacity(),
            key2, key2.position(), key2.capacity());
    }
}
