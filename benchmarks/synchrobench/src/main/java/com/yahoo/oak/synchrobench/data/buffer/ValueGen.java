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
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import net.openhft.chronicle.bytes.Bytes;
import net.spy.memcached.CachedData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Value generator for the buffer key/value.
 * It is a wrapper for all the methods that are implemented by the common class.
 */
public class ValueGen extends KeyValueGenerator implements ValueGenerator {
    public ValueGen() {
        super(Parameters.confValSize);
    }

    /** {@inheritDoc} **/
    @Override
    public void updateValue(BenchValue obj) {
        update((KeyValueBuffer) obj);
    }

    /** {@inheritDoc} **/
    @Override
    public void consumeValue(BenchValue obj, Blackhole blackhole) {
        read((KeyValueBuffer) obj, blackhole);
    }

    /** {@inheritDoc} **/
    @Override
    public void consumeSerializedValue(OakBuffer buffer, Blackhole blackhole) {
        readSerialized(buffer, blackhole);
    }

    /** {@inheritDoc} **/
    @Override
    public void serialize(BenchValue object, OakScopedWriteBuffer targetBuffer) {
        serialize((KeyValueBuffer) object, targetBuffer);
    }

    /** {@inheritDoc} **/
    @Override
    public int calculateSize(BenchValue object) {
        return calculateSize((KeyValueBuffer) object);
    }

    /** {@inheritDoc} **/
    @Override
    public int calculateHash(BenchValue object) {
        return calculateHash((KeyValueBuffer) object);
    }

    /** {@inheritDoc} **/
    @NotNull
    @Override
    public BenchValue read(Bytes in, @Nullable BenchValue using) {
        return readBytes(in, using);
    }

    /** {@inheritDoc} **/
    @Override
    public void write(Bytes out, @NotNull BenchValue toWrite) {
        writeBytes(out, toWrite);
    }

    /** {@inheritDoc} **/
    @Override
    public CachedData encode(BenchValue o) {
        return encodeBuffer((KeyValueBuffer) o);
    }

    /** {@inheritDoc} **/
    @Override
    public BenchValue decode(CachedData d) {
        return decodeBuffer(d);
    }
}
