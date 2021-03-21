/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.common.intbuffer.OakIntBufferComparator;
import com.yahoo.oak.common.intbuffer.OakIntBufferSerializer;

import java.nio.ByteBuffer;
import java.util.function.IntPredicate;

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

    public static void serialize(MyBuffer inputBuffer, OakScopedWriteBuffer targetBuffer) {
        serialize(inputBuffer, targetBuffer.getAddress());
    }

    public static void serialize(MyBuffer inputBuffer, long address) {
        // In the serialized buffer, the first integer signifies the size.
        UnsafeUtils.getUnsafe().putInt(address, inputBuffer.capacity);    
        OakIntBufferSerializer.copyBuffer(inputBuffer.buffer, DATA_POS, inputBuffer.capacity / Integer.BYTES,
                address + Integer.BYTES);
    }

    public static MyBuffer deserialize(OakScopedReadBuffer inputBuffer) {
        return deserialize(inputBuffer.getAddress());
    }

    public static MyBuffer deserialize(long address) {
        // In the serialized buffer, the first integer signifies the size.
        int capacity = UnsafeUtils.getUnsafe().getInt(address);
        // Thus, the data position starts after the first integer.
        MyBuffer ret = new MyBuffer(capacity);
        OakIntBufferSerializer.copyBuffer(address + Integer.BYTES, capacity / Integer.BYTES, ret.buffer, DATA_POS);
        return ret;
    }

    public static int compareBuffers(ByteBuffer buff1, int pos1, int cap1, ByteBuffer buff2, int pos2, int cap2) {
        return OakIntBufferComparator.compare(buff1, pos1, cap1 / Integer.BYTES,
                buff2, pos2, cap2 / Integer.BYTES);
    }
    
    public static int compareBuffers(ByteBuffer buff1, int pos1, int cap1, long buff2, int cap2) {
        return OakIntBufferComparator.compare(buff1, pos1, cap1 / Integer.BYTES, buff2, cap2 / Integer.BYTES);
    }
    
    public static int compareBuffers(long buff1, int cap1, long buff2, int cap2) {
        return OakIntBufferComparator.compare(buff1, cap1 / Integer.BYTES, buff2, cap2 / Integer.BYTES);
    }

    public static int compareBuffers(OakScopedReadBuffer buffer1, OakScopedReadBuffer buffer2) {
    	long address1 = buffer1.getAddress();
    	int cap1 = UnsafeUtils.getUnsafe().getInt( address1);
    	long address2 = buffer2.getAddress();
    	int cap2 = UnsafeUtils.getUnsafe().getInt( address2);
        return compareBuffers(address1 + Integer.BYTES, cap1, address2+ Integer.BYTES, cap2);
    }

    public static int compareBuffers(MyBuffer key1, OakScopedReadBuffer buffer2) {
        // In the serialized buffer, the first integer signifies the size.
    	long address = buffer2.getAddress();
    	int cap2 = UnsafeUtils.getUnsafe().getInt( address);
        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, address + Integer.BYTES, cap2);
    }

    public static int compareBuffers(MyBuffer key1, MyBuffer key2) {
        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, key2.buffer, DATA_POS, key2.capacity);
    }

    public static final OakSerializer<MyBuffer> DEFAULT_SERIALIZER = new OakSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer key, OakScopedWriteBuffer targetBuffer) {
            MyBuffer.serialize(key, targetBuffer);
        }

        @Override
        public MyBuffer deserialize(OakScopedReadBuffer serializedKey) {
            return MyBuffer.deserialize(serializedKey);
        }

        @Override
        public int calculateSize(MyBuffer object) {
            return object.calculateSerializedSize();
        }
    };

    public static final OakComparator<MyBuffer> DEFAULT_COMPARATOR = new OakComparator<MyBuffer>() {
        @Override
        public int compareKeys(MyBuffer key1, MyBuffer key2) {
            return MyBuffer.compareBuffers(key1, key2);
        }

        @Override
        public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
            return compareBuffers( serializedKey1,  serializedKey2);
        }

        @Override
        public int compareKeyAndSerializedKey(MyBuffer key, OakScopedReadBuffer serializedKey) {
            return compareBuffers(key,  serializedKey);
        }
    };
}
