/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.buffer;

import com.yahoo.oak.OakBuffer;
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

public class ValueGen extends KeyValueGenerator implements ValueGenerator {
    public ValueGen(Integer size) {
        super(size);
    }

    @Override
    public void updateValue(BenchValue obj) {
        update((KeyValueBuffer) obj);
    }

    @Override
    public void consumeValue(BenchValue obj, Blackhole blackhole) {
        read((KeyValueBuffer) obj, blackhole);
    }

    @Override
    public void consumeSerializedValue(OakBuffer buffer, Blackhole blackhole) {
        readSerialized(buffer, blackhole);
    }

    @Override
    public void serialize(BenchValue object, OakScopedWriteBuffer targetBuffer) {
        serialize((KeyValueBuffer) object, targetBuffer);
    }

    @Override
    public int calculateSize(BenchValue object) {
        return calculateSize((KeyValueBuffer) object);
    }

    @Override
    public int calculateHash(BenchValue object) {
        return calculateHash((KeyValueBuffer) object);
    }

    @NotNull
    @Override
    public BenchValue read(Bytes in, @Nullable BenchValue using) {
        return readBytes(in, using);
    }

    @Override
    public void write(Bytes out, @NotNull BenchValue toWrite) {
        writeBytes(out, toWrite);
    }

    @Override
    public CachedData encode(BenchValue o) {
        KeyValueBuffer buff = (KeyValueBuffer) o;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream os = null;
        try {
            bos = new ByteArrayOutputStream();
            os = new ObjectOutputStream(bos);

            os.writeInt(buff.capacity);
            for (int i = 0; i < buff.capacity; i++) {
                os.writeByte(buff.buffer.get(i));
            }

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

            int capacity = is.readInt();
            KeyValueBuffer buff = new KeyValueBuffer(capacity);
            for (int i = 0; i < buff.capacity; i++) {
                buff.buffer.put(i, is.readByte());
            }

            is.close();
            bis.close();
            return buff;
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot decode value", e);
        } finally {
            CloseUtil.close(is);
            CloseUtil.close(bis);
        }
    }
}
