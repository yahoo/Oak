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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class ByteBufferBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"ONHEAP","OFFHEAP"})
        String heap;

        ByteBuffer byteBuffer;

        int bytes = 1024*1024*1024;

        @Setup()
        public void setup() {
            if (heap.equals("ONHEAP")) {
                byteBuffer = ByteBuffer.allocate(bytes);
            } else {
                byteBuffer = ByteBuffer.allocateDirect(bytes);
            }
        }
    }


    @Warmup(iterations = 1)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1)
    @Threads(1)
    @Benchmark
    public void put(Blackhole blackhole, BenchmarkState state) {
        for (int i=0; i < state.bytes; ++i) {
            state.byteBuffer.put(i, (byte) i);
        }
    }

    @Warmup(iterations = 1)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1)
    @Threads(1)
    @Benchmark
    public void get(Blackhole blackhole, BenchmarkState state) {
        for (int i=0; i < state.bytes; ++i) {
            blackhole.consume(state.byteBuffer.get(i));
        }
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteBufferBenchmark.class.getSimpleName())
                .forks(1)
                .threads(1)
                .build();

        new Runner(opt).run();
    }

}
