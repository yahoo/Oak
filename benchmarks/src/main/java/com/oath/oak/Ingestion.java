
package com.oath.oak;

import javafx.util.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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


import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Ingestion {

    static public final int KEY_SIZE_BYTES = 64;
    static public final int VALUE_SIZE_BYTES = 4096;

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        private OakMap oakMap;

        @Setup(Level.Iteration)
        public void setup() {
            OakMapBuilder builder = new OakMapBuilder()
                    .setKeySerializer(new StringSerializer())
                    .setValueSerializer(new StringSerializer())
                    .setComparator(new StringComparator())
                    .setMinKey("")
                    .setChunkBytesPerItem(KEY_SIZE_BYTES);

            oakMap = builder.build();
        }

        @TearDown
        public void tearDown() {
            System.out.println("Num objects at tearDown " + oakMap.entries());
        }

        @TearDown(Level.Iteration)
        public void closeOak() {
            oakMap.close();
        }

    }

    @State(Scope.Thread)
    public static class ThreadState {

        @Param({"100000"})
        private int numRows;

        private ArrayList<Pair<String, String>> rows;

        @Setup
        public void setup() {
            rows = new ArrayList<>(numRows);
            for(int i = 0; i< numRows; ++i) {
                int keyPadding= KEY_SIZE_BYTES/Character.BYTES
                        - String.valueOf(Thread.currentThread().getId()).length()
                        - String.valueOf(i).length();
                String key = String.format(Thread.currentThread().getId() + "%0" + keyPadding +"d", i);

                int valuePadding= VALUE_SIZE_BYTES/Character.BYTES
                        - String.valueOf(Thread.currentThread().getId()).length()
                        - String.valueOf(i).length();

                String val = String.format("%0" + valuePadding + "d", i) + Thread.currentThread().getId();
                rows.add(new Pair<>(key, val));
            }
        }
    }


    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(1)
    @Threads(8)
    @Benchmark
    /*
     * Benchmark         (numRows)  Mode  Cnt    Score    Error  Units
     * Ingestion.Ingest     100000  avgt   10  339.849 ± 17.712  ms/op
     */
    public void Ingest(Blackhole blackhole,BenchmarkState state,ThreadState threadState) throws Exception {
        for (int i = 0; i < threadState.numRows; ++i) {
            Pair<String, String> pair = threadState.rows.get(i);
            state.oakMap.put(pair.getKey(), pair.getValue());
            blackhole.consume(state.oakMap);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Ingestion.class.getSimpleName())
                .threads(16)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
