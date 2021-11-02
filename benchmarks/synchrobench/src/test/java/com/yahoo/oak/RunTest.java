/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.contention.benchmark.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class RunTest {

    public static final List<String> BENCH = Arrays.asList(
        "JavaSkipListMap",
        "JavaHashMap",
        "OakBenchMap",
        "OakBenchHash",
        "OffHeapList",
        "Chronicle"
    );

    public static final List<String> DATA = Arrays.asList(
        "eventcache",
        "buffer"
    );

    public static final Map<String, String[]> SCENARIOS = Stream.of(new String[][]{
        {"4a-put", "-a", "0", "-u", "100"},
        {"4b-putIfAbsentComputeIfPresent", "--buffer", "-u", "0", "-s", "100", "-c"},
        {"4c-get-zc", "--buffer"},
        {"4c-get-copy"},
        {"4d-95Get5Put", "--buffer", "-a", "0", "-u", "5"},
        {"4e-entrySet-ascend", "--buffer", "-c"},
        {"4e-entryStreamSet-ascend", "--buffer", "-c", "--stream-iteration"},
        {"4f-entrySet-descend", "--buffer", "-c", "-a", "100"},
        {"4f-entryStreamSet-descend", "--buffer", "-c", "-a", "100", "--stream-iteration"},
        {"not-random-put", "-a", "0", "-u", "100", "--inc"}
    }).collect(
        Collectors.toMap(data -> data[0], data -> Arrays.copyOfRange(data, 1, data.length))
    );

    String[] args;

    public RunTest(String bench, String keyValue, String scenario) {
        this.args = getArgs(bench, keyValue, scenario, 10, 1);
    }

    public static String[] getArgs(String bench, String keyValue, String scenario, int duration, int threads) {
        return Stream.concat(Arrays.stream(new String[]{
                "-b", "com.yahoo.oak." + bench,
                "--key", "com.yahoo.oak.synchrobench.data." + keyValue, "-k", "32",
                "--value", "com.yahoo.oak.synchrobench.data." + keyValue, "-v", "64",
                "-i", "100",
                "-r", "200",
                "-t", Integer.toString(threads),
                "-W", "10",
                "-n", "1",
                "-d", Integer.toString(duration),
                "--consume-keys",
                "--consume-values",
                "--small-footprint",
                "--latency"
            }), Arrays.stream(
                SCENARIOS.get(scenario))
        ).toArray(String[]::new);
    }

    /**
     * Generates all the permutations of the parameters.
     *
     * @param c a list of collections of parameters
     * @return the cartesian product of all parameters
     */
    private static List<Object[]> cartesianProduct(Collection<?>... c) {
        final ArrayList<Object[]> res = new ArrayList<>();
        final int curLength = c.length;

        if (curLength == 0) {
            res.add(new Object[0]);
            return res;
        }

        final int curItem = curLength - 1;
        for (Object[] objList : cartesianProduct(Arrays.copyOfRange(c, 0, curItem))) {
            for (Object o : c[curItem]) {
                Object[] newObjList = Arrays.copyOf(objList, curLength);
                newObjList[curItem] = o;
                res.add(newObjList);
            }
        }

        return res;
    }

    @Parameterized.Parameters(name = "{0}, {1}, {2}")
    public static List<Object[]> parameters() {
        return cartesianProduct(BENCH, DATA, SCENARIOS.keySet());
    }

    @org.junit.Test
    public void testRun() throws Exception {
        Test.main(args);
    }

    // public static void main(String[] argv) throws Exception {
    //     Test.main(
    //         getArgs("Chronicle", "eventcache", "4c-get-copy", 1_000, 4)
    //     );
    // }
}
