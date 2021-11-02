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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.infra.Blackhole;

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
        throw new UnsupportedOperationException("Buffer does not support memcached");
    }

    @Override
    public BenchValue decode(CachedData d) {
        throw new UnsupportedOperationException("Buffer does not support memcached");
    }
}
