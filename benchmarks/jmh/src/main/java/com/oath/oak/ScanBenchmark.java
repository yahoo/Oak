/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Threads(8)
@State(Scope.Benchmark)
public class ScanBenchmark
{

    static public final int KEY_SIZE_BYTES = 4;
    static public final int VALUE_SIZE_BYTES = 4;

    private OakMap<String, String> oakMap;

    @Param({"1000000"})
    private int numRows;

    @Setup
    public void setup() {

        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("")
                .setChunkBytesPerItem(KEY_SIZE_BYTES);

        oakMap = builder.build();

        for(int i = 0; i< numRows; ++i) {
            String key = String.format("%0$" + KEY_SIZE_BYTES/Character.BYTES +"s",
                    String.valueOf(i) + Thread.currentThread().getId());

            String val = String.format("%0$-" + VALUE_SIZE_BYTES/Character.BYTES +"s",
                    String.valueOf(i) + Thread.currentThread().getId());

            oakMap.zc().put(key, val);
        }
    }

    @TearDown
    public void closeOak() {
        oakMap.close();
    }

    @Benchmark
    public void scan(Blackhole blackhole) {
        Iterator<String> iterator = oakMap.keySet().iterator();

        while (iterator.hasNext()) {
            String val = iterator.next();
            blackhole.consume(val);
        }

    }

    @Benchmark
    public void bufferViewScan(Blackhole blackhole) {
       Iterator<OakRBuffer> iterator = oakMap.zc().keySet().iterator();
        while (iterator.hasNext()) {
            OakRBuffer val = iterator.next();
            blackhole.consume(val);
        }
    }

    @Benchmark
    public void inverseScan(Blackhole blackhole) {
        try (OakMap inverseMap = oakMap.descendingMap()) {
            Iterator<String> iterator = inverseMap.keySet().iterator();
            while (iterator.hasNext()) {
                String val = iterator.next();
                blackhole.consume(val);
            }
        }
    }

    @Benchmark
    public void inverseBufferViewScan(Blackhole blackhole) {
        try (OakMap inverseMap = oakMap.descendingMap()) {
            Iterator<ByteBuffer> iterator = inverseMap.zc().keySet().iterator();
            while (iterator.hasNext()) {
                ByteBuffer val = iterator.next();
                blackhole.consume(val);
            }
        }
    }

    //java -jar -Xmx8g -XX:MaxDirectMemorySize=8g ./benchmarks/target/benchmarks.jar Scan -p numRows=1000000 -prof stack
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include("scan")
                .forks(0)
                .threads(8)
                .build();

        new Runner(opt).run();
    }

}
