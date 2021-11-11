/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.eventcache;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import net.openhft.chronicle.bytes.Bytes;
import net.spy.memcached.CachedData;
import net.spy.memcached.compat.CloseUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

public class ValueGen implements ValueGenerator {
    private static final int FIELD_1_OFFSET = 0;
    private static final int FIELD_2_OFFSET = FIELD_1_OFFSET + Float.BYTES;
    private static final int FIELD_3_OFFSET = FIELD_2_OFFSET + Float.BYTES;
    private static final int VAL_SIZE = FIELD_3_OFFSET + Integer.BYTES;

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
        val.setField3(~val.getField3());
    }

    @Override
    public void updateSerializedValue(OakScopedWriteBuffer b) {
        b.putInt(FIELD_3_OFFSET, ~b.getInt(FIELD_3_OFFSET));
    }

    @Override
    public void consumeValue(BenchValue obj, Blackhole blackhole) {
        Value val = (Value) obj;
        blackhole.consume(val.getField1());
        blackhole.consume(val.getField2());
        blackhole.consume(val.getField3());
    }

    @Override
    public void consumeSerializedValue(OakBuffer b, Blackhole blackhole) {
        blackhole.consume(b.getFloat(FIELD_1_OFFSET));
        blackhole.consume(b.getFloat(FIELD_2_OFFSET));
        blackhole.consume(b.getInt(FIELD_3_OFFSET));
    }

    @Override
    public void serialize(BenchValue inputVal, OakScopedWriteBuffer targetBuffer) {
        Value val = (Value) inputVal;
        storeValue(targetBuffer, val.getField1(), val.getField2(), val.getField3());
    }

    @Override
    public BenchValue deserialize(OakScopedReadBuffer valBuffer) {
        return new Value(
            valBuffer.getFloat(FIELD_1_OFFSET),
            valBuffer.getFloat(FIELD_2_OFFSET),
            valBuffer.getInt(FIELD_3_OFFSET)
        );
    }

    @Override
    public boolean asyncDecode(CachedData d) {
        return false;
    }

    @Override
    public CachedData encode(BenchValue o) {
        Value val = (Value) o;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream os = null;
        try {
            bos = new ByteArrayOutputStream();
            os = new ObjectOutputStream(bos);

            os.writeFloat(val.field1);
            os.writeFloat(val.field2);
            os.writeInt(val.field3);

            os.close();
            bos.close();

            byte[] data = bos.toByteArray();
            return new CachedData(0, data, data.length);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot encode value", e);
        } finally {
            CloseUtil.close(os);
            CloseUtil.close(bos);
        }
    }

    @Override
    public BenchValue decode(CachedData d) {
        byte[] data = d.getData();
        ByteArrayInputStream bis = null;
        ObjectInputStream is = null;
        try {
            bis = new ByteArrayInputStream(data);
            is = new ObjectInputStream(bis);

            BenchValue value = new Value(
                is.readFloat(),
                is.readFloat(),
                is.readInt()
            );

            is.close();
            bis.close();
            return value;
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot decode value", e);
        } finally {
            CloseUtil.close(is);
            CloseUtil.close(bis);
        }
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
        targetBuffer.putFloat(FIELD_1_OFFSET, val1);
        targetBuffer.putFloat(FIELD_2_OFFSET, val2);
        targetBuffer.putInt(FIELD_3_OFFSET, val3);
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
        value.field1 = in.readFloat();
        value.field2 = in.readFloat();
        value.field3 = in.readInt();
        return value;
    }

    @Override
    public void write(Bytes out, @NotNull BenchValue toWrite) {
        Value value = (Value) toWrite;
        out.writeFloat(value.field1);
        out.writeFloat(value.field2);
        out.writeInt(value.field3);
    }
}
