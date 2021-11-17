/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.data.buffer;

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Key generator for the buffer key/value.
 * It is a wrapper for all the methods that are implemented by the common class.
 */
public class KeyGen extends KeyValueGenerator implements KeyGenerator {
    public KeyGen() {
        super(Parameters.confKeySize);
    }

    /** {@inheritDoc} **/
    @Override
    public void consumeKey(BenchKey obj, Blackhole blackhole) {
        read((KeyValueBuffer) obj, blackhole);
    }

    /** {@inheritDoc} **/
    @Override
    public void consumeSerializedKey(OakBuffer buffer, Blackhole blackhole) {
        readSerialized(buffer, blackhole);
    }

    /** {@inheritDoc} **/
    @Override
    public int compare(BenchKey key1, BenchKey key2) {
        return KeyGenerator.super.compare(key1, key2);
    }

    /** {@inheritDoc} **/
    @Override
    public int compareKeys(BenchKey key1, BenchKey key2) {
        return compareKeys((KeyValueBuffer) key1, (KeyValueBuffer) key2);
    }

    /** {@inheritDoc} **/
    @Override
    public int compareKeyAndSerializedKey(BenchKey key1, OakScopedReadBuffer serializedKey) {
        return compareKeyAndSerializedKey((KeyValueBuffer) key1, serializedKey);
    }

    @Override
    public void serialize(BenchKey object, OakScopedWriteBuffer targetBuffer) {
        serialize((KeyValueBuffer) object, targetBuffer);
    }

    /** {@inheritDoc} **/
    @Override
    public int calculateSize(BenchKey object) {
        return calculateSize((KeyValueBuffer) object);
    }

    /** {@inheritDoc} **/
    @Override
    public int calculateHash(BenchKey object) {
        return calculateHash((KeyValueBuffer) object);
    }

    /** {@inheritDoc} **/
    @NotNull
    @Override
    public BenchKey read(Bytes in, long size, @Nullable BenchKey using) {
        return readBytes(in, using);
    }

    /** {@inheritDoc} **/
    @Override
    public void write(Bytes out, long size, @NotNull BenchKey toWrite) {
        writeBytes(out, toWrite);
    }
}
