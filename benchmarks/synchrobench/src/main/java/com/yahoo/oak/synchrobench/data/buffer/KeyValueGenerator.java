/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.buffer;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.common.intbuffer.OakIntBufferComparator;
import com.yahoo.oak.common.intbuffer.OakIntBufferSerializer;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Random;

public class KeyValueGenerator {

    private static final int DATA_POS = 0;

    public final int size;

    public KeyValueGenerator(Integer size) {
        this.size = size;
    }

    public void serialize(KeyValueBuffer buff, OakScopedWriteBuffer targetBuffer) {
        // In the serialized buffer, the first integer signifies the size.
        int targetPos = 0;
        targetBuffer.putInt(targetPos, buff.capacity);
        targetPos += Integer.BYTES;
        OakIntBufferSerializer.copyBuffer(buff.buffer, DATA_POS, buff.capacity / Integer.BYTES,
            targetBuffer, targetPos);
    }

    public KeyValueBuffer deserialize(OakScopedReadBuffer inputBuffer) {
        int inputPos = 0;
        int capacity = inputBuffer.getInt(inputPos);
        inputPos += Integer.BYTES;
        KeyValueBuffer ret = new KeyValueBuffer(capacity);
        OakIntBufferSerializer.copyBuffer(inputBuffer, inputPos, capacity / Integer.BYTES, ret.buffer, DATA_POS);
        return ret;
    }

    public String toString(BenchKey obj) {
        throw new UnsupportedOperationException("Buffer does not support memcached");
    }

    @NotNull
    public KeyValueBuffer readBytes(Bytes in, @Nullable Object using) {
        int capacity = in.readInt();

        KeyValueBuffer buff;
        if (using == null) {
            buff = new KeyValueBuffer(capacity);
        } else {
            buff = (KeyValueBuffer) using;
            assert buff.capacity == capacity;
        }

        for (int i = 0; i < buff.capacity; i++) {
            buff.buffer.put(i, in.readByte());
        }

        return buff;
    }

    public void writeBytes(Bytes out, @NotNull Object toWrite) {
        // In the serialized buffer, the first integer signifies the size.
        KeyValueBuffer buff = (KeyValueBuffer) toWrite;
        out.writeInt(buff.capacity);
        for (int i = 0; i < buff.capacity; i++) {
            out.writeByte(buff.buffer.get(i));
        }
    }

    public int calculateSize(KeyValueBuffer buff) {
        return buff.capacity + Integer.BYTES;
    }

    public int calculateHash(KeyValueBuffer object) {
        return object.hashCode();
    }

    private static int compareBuffers(ByteBuffer buff1, int pos1, int cap1, ByteBuffer buff2, int pos2, int cap2) {
        return OakIntBufferComparator.compare(buff1, pos1, cap1 / Integer.BYTES,
                buff2, pos2, cap2 / Integer.BYTES);
    }

    private static int compareBuffers(ByteBuffer buff1, int pos1, int cap1, OakScopedReadBuffer buff2,
                                      int pos2, int cap2) {
        return OakIntBufferComparator.compare(buff1, pos1, cap1 / Integer.BYTES,
                buff2, pos2, cap2 / Integer.BYTES);
    }

    private static int compareBuffers(OakScopedReadBuffer buff1, int pos1, int cap1, OakScopedReadBuffer buff2,
                                      int pos2, int cap2) {
        return OakIntBufferComparator.compare(buff1, pos1, cap1 / Integer.BYTES, buff2, pos2, cap2 / Integer.BYTES);
    }

    public static int compareBuffers(OakScopedReadBuffer buffer1, OakScopedReadBuffer buffer2) {
        // In the serialized buffer, the first integer signifies the size.
        int cap1 = buffer1.getInt(0);
        int cap2 = buffer2.getInt(0);
        return compareBuffers(buffer1, Integer.BYTES, cap1, buffer2, Integer.BYTES, cap2);
    }

    public static int compareBuffers(KeyValueBuffer key1, OakScopedReadBuffer buffer2) {
        // In the serialized buffer, the first integer signifies the size.
        int cap2 = buffer2.getInt(0);
        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, buffer2, Integer.BYTES, cap2);
    }

    public static int compareBuffers(KeyValueBuffer key1, KeyValueBuffer key2) {
        return compareBuffers(key1.buffer, DATA_POS, key1.capacity, key2.buffer, DATA_POS, key2.capacity);
    }

    public BenchKey getMinKey() {
        KeyValueBuffer minKey = new KeyValueBuffer(size);
        minKey.buffer.putInt(0, Integer.MIN_VALUE);
        return minKey;
    }

    public BenchKey getNextKey(Random rnd, int range, BenchKey prev) {
        KeyValueBuffer nextValue = new KeyValueBuffer(size);
        if (rnd == null) {
            int prevValue = prev != null ? ((KeyValueBuffer) prev).buffer.getInt(0) : 0;
            nextValue.buffer.putInt(0, (prevValue + 1) % range);
        } else {
            nextValue.buffer.putInt(0, rnd.nextInt(range));
        }
        return nextValue;
    }

    public BenchValue getNextValue(Random rnd, int range) {
        KeyValueBuffer nextValue = new KeyValueBuffer(size);
        if (rnd == null) {
            nextValue.buffer.putInt(0, 0);
        } else {
            nextValue.buffer.putInt(0, rnd.nextInt(range));
        }
        return nextValue;
    }

    public void update(KeyValueBuffer buff) {
        buff.buffer.putLong(1, ~buff.buffer.getLong(1));
    }

    public void updateSerializedValue(OakScopedWriteBuffer b) {
        b.putLong(1, ~b.getLong(1));
    }

    public void read(KeyValueBuffer buff, Blackhole blackhole) {
        for (int i = 0; i < buff.capacity; i += Long.BYTES) {
            blackhole.consume(buff.buffer.getLong(i));
        }
    }

    public void readSerialized(OakBuffer b, Blackhole blackhole) {
        int capacity = b.getInt(0);
        for (int i = 0; i < capacity; i += Long.BYTES) {
            blackhole.consume(b.getLong(Integer.BYTES + i));
        }
    }

    public int compareKeys(KeyValueBuffer key1, KeyValueBuffer key2) {
        return compareBuffers(key1, key2);
    }

    public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
        return compareBuffers(serializedKey1, serializedKey2);
    }

    public int compareKeyAndSerializedKey(KeyValueBuffer key, OakScopedReadBuffer serializedKey) {
        return compareBuffers(key, serializedKey);
    }
}
