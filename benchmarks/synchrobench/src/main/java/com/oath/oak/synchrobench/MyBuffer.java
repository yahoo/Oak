package com.oath.oak.synchrobench;


import com.oath.oak.OakComparator;
import com.oath.oak.OakReadBuffer;
import com.oath.oak.OakSerializer;
import com.oath.oak.OakUnsafeDirectBuffer;
import com.oath.oak.OakWriteBuffer;
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

    public static void serialize(MyBuffer inputBuffer, OakUnsafeDirectBuffer targetBuffer) {
        serialize(inputBuffer, targetBuffer.getByteBuffer(), targetBuffer.getOffset());
    }

    public static void serialize(MyBuffer inputBuffer, ByteBuffer targetBuffer, int targetPos) {
        // In the serialized buffer, the first integer signifies the size.
        targetBuffer.putInt(targetPos, inputBuffer.capacity);
        // Thus, the data position starts after the first integer.
        targetPos += Integer.BYTES;

        OakIntBufferSerializer.copyBuffer(inputBuffer.buffer, DATA_POS, inputBuffer.capacity / Integer.BYTES,
                targetBuffer, targetPos);
    }

    public static MyBuffer deserialize(OakUnsafeDirectBuffer inputBuffer) {
        return deserialize(inputBuffer.getByteBuffer(), inputBuffer.getOffset());
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

    public static int compareBuffers(OakUnsafeDirectBuffer buffer1, OakUnsafeDirectBuffer buffer2) {
        ByteBuffer buf1 = buffer1.getByteBuffer();
        ByteBuffer buf2 = buffer2.getByteBuffer();

        int pos1 = buffer1.getOffset();
        int pos2 = buffer2.getOffset();

        // In the serialized buffer, the first integer signifies the size.
        int cap1 = buf1.getInt(pos1);
        int cap2 = buf2.getInt(pos2);

        // Thus, the data position starts after the first integer.
        pos1 += Integer.BYTES;
        pos2 += Integer.BYTES;

        return compareBuffers(buf1, pos1, cap1, buf2, pos2, cap2);
    }

    public static int compareBuffers(MyBuffer key1, OakUnsafeDirectBuffer buffer2) {
        ByteBuffer buf2 = buffer2.getByteBuffer();

        int pos2 = buffer2.getOffset();

        // In the serialized buffer, the first integer signifies the size.
        int cap2 = buf2.getInt(pos2);

        // Thus, the data position starts after the first integer.
        pos2 += Integer.BYTES;

        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, buf2, pos2, cap2);
    }

    public static int compareBuffers(MyBuffer key1, MyBuffer key2) {
        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, key2.buffer, DATA_POS, key2.capacity);
    }

    public static OakSerializer<MyBuffer> defaultSerializer = new OakSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer key, OakWriteBuffer targetBuffer) {
            MyBuffer.serialize(key, (OakUnsafeDirectBuffer) targetBuffer);
        }

        @Override
        public MyBuffer deserialize(OakReadBuffer serializedKey) {
            return MyBuffer.deserialize((OakUnsafeDirectBuffer) serializedKey);
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
        public int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2) {
            return compareBuffers((OakUnsafeDirectBuffer) serializedKey1, (OakUnsafeDirectBuffer) serializedKey2);
        }

        @Override
        public int compareKeyAndSerializedKey(MyBuffer key, OakReadBuffer serializedKey) {
            return compareBuffers(key, (OakUnsafeDirectBuffer) serializedKey);
        }
    };
}
