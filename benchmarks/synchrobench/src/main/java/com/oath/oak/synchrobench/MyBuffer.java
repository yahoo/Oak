package com.oath.oak.synchrobench;


import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;
import com.oath.oak.common.intbuffer.OakIntBufferComparator;
import com.oath.oak.common.intbuffer.OakIntBufferSerializer;

import java.nio.ByteBuffer;

public class MyBuffer implements Comparable<MyBuffer> {

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
        // In the serialized buffer, the first integer signifies the size.
        targetBuffer.putInt(targetPos, inputBuffer.capacity);
        // Thus, the data position starts after the first integer.
        targetPos += Integer.BYTES;

        OakIntBufferSerializer.copyBuffer(inputBuffer.buffer, 0, inputBuffer.capacity / Integer.BYTES,
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
        OakIntBufferSerializer.copyBuffer(inputBuffer, inputPos, capacity / Integer.BYTES, ret.buffer, 0);
        return ret;
    }

    public static int calculateSerializedSize(MyBuffer object) {
        return object.buffer.capacity() + Integer.BYTES;
    }

    public static int compareBuffers(ByteBuffer buff1, int pos1, int cap1, ByteBuffer buff2, int pos2, int cap2) {
        return OakIntBufferComparator.compare(buff1, pos1, cap1 / Integer.BYTES,
            buff2, pos2, cap2 / Integer.BYTES);
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


    public static OakSerializer<MyBuffer> defaultSerializer = new OakSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer key, ByteBuffer targetBuffer) {
            MyBuffer.serialize(key, targetBuffer, targetBuffer.position());
        }

        @Override
        public MyBuffer deserialize(ByteBuffer serializedKey) {
            return MyBuffer.deserialize(serializedKey, serializedKey.position());
        }

        @Override
        public int calculateSize(MyBuffer object) {
            return MyBuffer.calculateSerializedSize(object);
        }
    };

    public static OakComparator<MyBuffer> defaultComparator = new OakComparator<MyBuffer>() {
        @Override
        public int compareKeys(MyBuffer key1, MyBuffer key2) {
            return MyBuffer.compareBuffers(key1, key2);
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            int pos1 = serializedKey1.position();
            int pos2 = serializedKey2.position();

            // In the serialized buffer, the first integer signifies the size.
            int cap1 = serializedKey1.getInt(pos1);
            int cap2 = serializedKey2.getInt(pos2);

            // Thus, the data position starts after the first integer.
            pos1 += Integer.BYTES;
            pos2 += Integer.BYTES;

            return compareBuffers(serializedKey1, pos1, cap1, serializedKey2, pos2, cap2);
        }

        @Override
        public int compareKeyAndSerializedKey(MyBuffer key, ByteBuffer serializedKey) {
            int keyPosition = key.buffer.position();
            int keyLength = key.buffer.capacity();
            int serializedKeyPosition = serializedKey.position();
            // In the serialized buffer, the first integer signifies the size.
            int serializedKeyLength = serializedKey.getInt(serializedKeyPosition);
            // Thus, the data position starts after the first integer.
            serializedKeyPosition += Integer.BYTES;

            // The order of the arguments is crucial and should match the signature of this function
            // (compareKeyAndSerializedKey).
            // Thus key.buffer with its parameters should be passed, and only then serializedKey with its parameters.
            return compareBuffers(key.buffer, keyPosition, keyLength,
                serializedKey, serializedKeyPosition, serializedKeyLength);

        }
    };
}
