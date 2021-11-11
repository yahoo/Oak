/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.eventcache;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.infra.Blackhole;

public class KeyGen implements KeyGenerator {
    private static final int FIELD_1_OFFSET = 0;
    private static final int FIELD_2_OFFSET = FIELD_1_OFFSET + Long.BYTES;
    private static final int KEY_SIZE = FIELD_2_OFFSET + Long.BYTES;

    public KeyGen(Integer keySize) {
    }

    @Override
    public BenchKey getMinKey() {
        return new Key(Long.MIN_VALUE, Long.MIN_VALUE);
    }

    @Override
    public BenchKey getNextKey(int keyIndex) {
        return new Key(0, keyIndex);
    }

    @Override
    public void serialize(BenchKey inputKey, OakScopedWriteBuffer targetBuffer) {
        Key key = (Key) inputKey;
        targetBuffer.putLong(FIELD_1_OFFSET, getField1(key));
        targetBuffer.putLong(FIELD_2_OFFSET, getField2(key));
    }

    @Override
    public BenchKey deserialize(OakScopedReadBuffer keyBuffer) {
        return new Key(getField1(keyBuffer), getField2(keyBuffer));
    }

    @Override
    public String toString(BenchKey obj) {
        return obj.toString();
    }

    @Override
    public int calculateSize(BenchKey key) {
        return KEY_SIZE;
    }

    @Override
    public int calculateHash(BenchKey key) {
        return key.hashCode();
    }

    @Override
    public int compareKeys(BenchKey inputKey1, BenchKey inputKey2) {
        Key key1 = (Key) inputKey1;
        Key key2 = (Key) inputKey2;
        int ret = Long.compare(getField1(key1), getField1(key2));
        return ret != 0 ? ret : Long.compare(getField2(key1), getField2(key2));
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer key1, OakScopedReadBuffer key2) {
        int ret = Long.compare(getField1(key1), getField1(key2));
        return ret != 0 ? ret : Long.compare(getField2(key1), getField2(key2));
    }

    @Override
    public int compareKeyAndSerializedKey(BenchKey inputKey1, OakScopedReadBuffer key2) {
        Key key1 = (Key) inputKey1;
        int ret = Long.compare(getField1(key1), getField1(key2));
        return ret != 0 ? ret : Long.compare(getField2(key1), getField2(key2));
    }

    @Override
    public void consumeKey(BenchKey obj, Blackhole blackhole) {
        Key val = (Key) obj;
        blackhole.consume(getField1(val));
        blackhole.consume(getField2(val));
    }

    @Override
    public void consumeSerializedKey(OakBuffer val, Blackhole blackhole) {
        blackhole.consume(getField1(val));
        blackhole.consume(getField2(val));
    }

    private static long getField1(Key key) {
        return key.getField1();
    }

    private static long getField1(OakBuffer key) {
        return key.getLong(FIELD_1_OFFSET);
    }

    private static long getField2(Key key) {
        return key.getField2();
    }

    private static long getField2(OakBuffer key) {
        return key.getLong(FIELD_2_OFFSET);
    }

    @NotNull
    @Override
    public BenchKey read(Bytes in, long size, @Nullable BenchKey using) {
        if (using == null) {
            return new Key(
                in.readLong(),
                in.readLong()
            );
        }

        Key key = (Key) using;
        key.field1 = in.readLong();
        key.field2 = in.readLong();
        return key;
    }

    @Override
    public void write(Bytes out, long size, @NotNull BenchKey toWrite) {
        Key key = (Key) toWrite;
        out.writeLong(key.field1);
        out.writeLong(key.field2);
    }
}
