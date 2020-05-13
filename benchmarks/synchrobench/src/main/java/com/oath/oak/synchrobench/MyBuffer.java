package com.oath.oak.synchrobench;


import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;
import com.oath.oak.common.intbuffer.OakIntBufferComparator;
import com.oath.oak.common.intbuffer.OakIntBufferSerializer;

import java.nio.ByteBuffer;

public class MyBuffer implements Comparable<MyBuffer> {

    private final static int DATA_POS = 0;

    public final int capacity;
    public final ByteBuffer buffer;

    public MyBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public int calculateSerializedSize() {
        return capacity + Integer.BYTES;
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
        // In the serialized buffer, the first integer signifies the size.
        targetBuffer.putInt(targetPos, inputBuffer.capacity);
        // Thus, the data position starts after the first integer.
        targetPos += Integer.BYTES;

        OakIntBufferSerializer.copyBuffer(inputBuffer.buffer, DATA_POS, inputBuffer.capacity / Integer.BYTES,
            targetBuffer, targetPos);
    }

    public static MyBuffer deserialize(ByteBuffer inputBuffer) {
        int inputPos = inputBuffer.position();
        return deserialize(inputBuffer, inputPos);
    }

    public static MyBuffer deserialize(ByteBuffer inputBuffer, int inputPos) {
        // In the serialized buffer, the first integer signifies the size.
        int capacity = inputBuffer.getInt(inputPos);
        // Thus, the data position starts after the first integer.
        inputPos += Integer.BYTES;

        MyBuffer ret = new MyBuffer(capacity);
        OakIntBufferSerializer.copyBuffer(inputBuffer, inputPos, capacity / Integer.BYTES, ret.buffer, DATA_POS);
        return ret;
    }

    public static int compareBuffers(ByteBuffer buff1, int pos1, int cap1, ByteBuffer buff2, int pos2, int cap2) {
        return OakIntBufferComparator.compare(buff1, pos1, cap1 / Integer.BYTES,
            buff2, pos2, cap2 / Integer.BYTES);
    }

    public static int compareBuffers(ByteBuffer buffer1, ByteBuffer buffer2) {
        int pos1 = buffer1.position();
        int pos2 = buffer2.position();

        // In the serialized buffer, the first integer signifies the size.
        int cap1 = buffer1.getInt(pos1);
        int cap2 = buffer2.getInt(pos2);

        // Thus, the data position starts after the first integer.
        pos1 += Integer.BYTES;
        pos2 += Integer.BYTES;

        return compareBuffers(buffer1, pos1, cap1, buffer2, pos2, cap2);
    }

    public static int compareBuffers(MyBuffer key1, ByteBuffer buffer2) {
        int pos2 = buffer2.position();

        // In the serialized buffer, the first integer signifies the size.
        int cap2 = buffer2.getInt(pos2);

        // Thus, the data position starts after the first integer.
        pos2 += Integer.BYTES;

        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, buffer2, pos2, cap2);
    }

    public static int compareBuffers(MyBuffer key1, MyBuffer key2) {
        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, key2.buffer, DATA_POS, key2.capacity);
    }

    public static OakSerializer<MyBuffer> defaultSerializer = new OakSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer key, ByteBuffer targetBuffer) {
            MyBuffer.serialize(key, targetBuffer);
        }

        @Override
        public MyBuffer deserialize(ByteBuffer serializedKey) {
            return MyBuffer.deserialize(serializedKey);
        }

        @Override
        public int calculateSize(MyBuffer object) {
            return object.calculateSerializedSize();
        }
    };

    public static OakComparator<MyBuffer> defaultComparator = new OakComparator<MyBuffer>() {
        @Override
        public int compareKeys(MyBuffer key1, MyBuffer key2) {
            return MyBuffer.compareBuffers(key1, key2);
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            return compareBuffers(serializedKey1, serializedKey2);
        }

        @Override
        public int compareKeyAndSerializedKey(MyBuffer key, ByteBuffer serializedKey) {
            return compareBuffers(key, serializedKey);
        }
    };
}
