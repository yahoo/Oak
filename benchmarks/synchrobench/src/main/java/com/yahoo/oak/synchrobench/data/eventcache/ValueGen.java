/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.eventcache;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import net.openhft.chronicle.bytes.Bytes;
import net.spy.memcached.CachedData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.infra.Blackhole;
import sun.misc.Unsafe;

import java.util.Random;

public class ValueGen implements ValueGenerator {
    private static final int VAL_1_OFFSET = 0;
    private static final int VAL_2_OFFSET = VAL_1_OFFSET + Float.BYTES;
    private static final int VAL_3_OFFSET = VAL_2_OFFSET + Float.BYTES;
    private static final int VAL_SIZE = VAL_3_OFFSET + Integer.BYTES;

    private static final long VAL_1_DATA_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;
    private static final long VAL_2_DATA_OFFSET = VAL_1_DATA_OFFSET +
        (long) Float.BYTES * Unsafe.ARRAY_BYTE_INDEX_SCALE;
    private static final long VAL_3_DATA_OFFSET = VAL_2_DATA_OFFSET +
        (long) Float.BYTES * Unsafe.ARRAY_BYTE_INDEX_SCALE;
    private static final int VAL_DATA_SIZE = (int) (VAL_3_DATA_OFFSET +
        (long) Integer.BYTES * Unsafe.ARRAY_BYTE_INDEX_SCALE);


    public ValueGen(Integer valueSize) {
    }

    @Override
    public BenchValue getNextValue(Random rnd, int range) {
        assert rnd != null;
        return new Value(
            rnd.nextFloat(),
            rnd.nextFloat(),
            rnd.nextInt(range)
        );
    }

    @Override
    public void updateValue(BenchValue obj) {
        Value val = (Value) obj;
        val.setVal3(~val.getVal3());
    }

    @Override
    public void updateSerializedValue(OakScopedWriteBuffer b) {
        b.putInt(VAL_3_OFFSET, ~b.getInt(VAL_3_OFFSET));
    }

    @Override
    public void consumeValue(BenchValue obj, Blackhole blackhole) {
        Value val = (Value) obj;
        blackhole.consume(val.getVal1());
        blackhole.consume(val.getVal2());
        blackhole.consume(val.getVal3());
    }

    @Override
    public void consumeSerializedValue(OakBuffer b, Blackhole blackhole) {
        blackhole.consume(b.getFloat(VAL_1_OFFSET));
        blackhole.consume(b.getFloat(VAL_2_OFFSET));
        blackhole.consume(b.getInt(VAL_3_OFFSET));
    }

    @Override
    public void serialize(BenchValue inputVal, OakScopedWriteBuffer targetBuffer) {
        Value val = (Value) inputVal;
        storeValue(targetBuffer, val.getVal1(), val.getVal2(), val.getVal3());
    }

    @Override
    public BenchValue deserialize(OakScopedReadBuffer valBuffer) {
        return new Value(
            valBuffer.getFloat(VAL_1_OFFSET),
            valBuffer.getFloat(VAL_2_OFFSET),
            valBuffer.getInt(VAL_3_OFFSET)
        );
    }

    @Override
    public boolean asyncDecode(CachedData d) {
        return false;
    }

    @Override
    public CachedData encode(BenchValue o) {
        Value val = (Value) o;
        byte[] data = new byte[VAL_SIZE];
        UnsafeUtils.UNSAFE.putFloat(data, VAL_1_DATA_OFFSET, val.val1);
        UnsafeUtils.UNSAFE.putFloat(data, VAL_2_DATA_OFFSET, val.val2);
        UnsafeUtils.UNSAFE.putInt(data, VAL_3_DATA_OFFSET, val.val3);
        return new CachedData(0, data, VAL_DATA_SIZE);
    }

    @Override
    public BenchValue decode(CachedData d) {
        byte[] data = d.getData();
        return new Value(
            UnsafeUtils.UNSAFE.getFloat(data, VAL_1_DATA_OFFSET),
            UnsafeUtils.UNSAFE.getFloat(data, VAL_2_DATA_OFFSET),
            UnsafeUtils.UNSAFE.getInt(data, VAL_3_DATA_OFFSET)
        );
    }

    @Override
    public int calculateSize(BenchValue key) {
        return VAL_SIZE;
    }

    @Override
    public int calculateHash(BenchValue key) {
        return 0;
    }

    public static void storeValue(OakScopedWriteBuffer targetBuffer,
                                  float val1, float val2, int val3) {
        targetBuffer.putFloat(VAL_1_OFFSET, val1);
        targetBuffer.putFloat(VAL_2_OFFSET, val2);
        targetBuffer.putInt(VAL_3_OFFSET, val3);
    }

    @NotNull
    @Override
    public BenchValue read(Bytes in, @Nullable BenchValue using) {
        if (using == null) {
            return new Value(
                in.readFloat(),
                in.readFloat(),
                in.readInt()
            );
        }

        Value value = (Value) using;
        value.val1 = in.readFloat();
        value.val2 = in.readFloat();
        value.val3 = in.readInt();
        return value;
    }

    @Override
    public void write(Bytes out, @NotNull BenchValue toWrite) {
        Value value = (Value) toWrite;
        out.writeFloat(value.val1);
        out.writeFloat(value.val2);
        out.writeInt(value.val3);
    }
}
