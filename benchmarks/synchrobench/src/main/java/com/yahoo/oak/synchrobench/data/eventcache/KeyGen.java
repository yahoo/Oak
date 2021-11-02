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

import java.util.Random;

public class KeyGen implements KeyGenerator {
    private static final int KEY_1_OFFSET = 0;
    private static final int KEY_2_OFFSET = KEY_1_OFFSET + Long.BYTES;
    private static final int KEY_SIZE = KEY_2_OFFSET + Long.BYTES;

    public KeyGen(Integer keySize) {
    }

    @Override
    public BenchKey getMinKey() {
        return new Key(Long.MIN_VALUE, Long.MIN_VALUE);
    }

    @Override
    public BenchKey getNextKey(Random rnd, int range, BenchKey prev) {
        final Key prevKey = prev == null ? null : (Key) prev;
        long key1 = prevKey == null ? 0 : prevKey.getKey1();
        long key2;

        if (rnd != null) {
            key2 = rnd.nextInt(range);
        } else {
            long prevKey2 = prevKey == null ? 0 : prevKey.getKey2();
            key2 = (prevKey2 + 1) % range;
        }
        return new Key(key1, key2);
    }

    @Override
    public void serialize(BenchKey inputKey, OakScopedWriteBuffer targetBuffer) {
        Key key = (Key) inputKey;
        targetBuffer.putLong(KEY_1_OFFSET, getKey1(key));
        targetBuffer.putLong(KEY_2_OFFSET, getKey2(key));
    }

    @Override
    public BenchKey deserialize(OakScopedReadBuffer keyBuffer) {
        return new Key(getKey1(keyBuffer), getKey2(keyBuffer));
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
        int ret = Long.compare(getKey1(key1), getKey1(key2));
        return ret != 0 ? ret : Long.compare(getKey2(key1), getKey2(key2));
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer key1, OakScopedReadBuffer key2) {
        int ret = Long.compare(getKey1(key1), getKey1(key2));
        return ret != 0 ? ret : Long.compare(getKey2(key1), getKey2(key2));
    }

    @Override
    public int compareKeyAndSerializedKey(BenchKey inputKey1, OakScopedReadBuffer key2) {
        Key key1 = (Key) inputKey1;
        int ret = Long.compare(getKey1(key1), getKey1(key2));
        return ret != 0 ? ret : Long.compare(getKey2(key1), getKey2(key2));
    }

    @Override
    public void consumeKey(BenchKey obj, Blackhole blackhole) {
        Key val = (Key) obj;
        blackhole.consume(getKey1(val));
        blackhole.consume(getKey2(val));
    }

    @Override
    public void consumeSerializedKey(OakBuffer val, Blackhole blackhole) {
        blackhole.consume(getKey1(val));
        blackhole.consume(getKey2(val));
    }

    private static long getKey1(Key key) {
        return key.getKey1();
    }

    private static long getKey1(OakBuffer key) {
        return key.getLong(KEY_1_OFFSET);
    }

    private static long getKey2(Key key) {
        return key.getKey2();
    }

    private static long getKey2(OakBuffer key) {
        return key.getLong(KEY_2_OFFSET);
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
        key.key1 = in.readLong();
        key.key2 = in.readLong();
        return key;
    }

    @Override
    public void write(Bytes out, long size, @NotNull BenchKey toWrite) {
        Key key = (Key) toWrite;
        out.writeLong(key.key1);
        out.writeLong(key.key2);
    }
}
