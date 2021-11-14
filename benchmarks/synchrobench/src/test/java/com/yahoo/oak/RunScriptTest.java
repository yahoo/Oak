/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import shaded.org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class RunScriptTest {

    Path tmpOutputDir;
    List<String> args;
    List<String> flags;
    List<Map<String, String>> expect;

    @Parameterized.Parameters(name = "{0}")
    public static List<List<String>> parameters() {
        return Arrays.asList(
            Collections.emptyList(),
            FLAGS
        );
    }

    public RunScriptTest(List<String> flags) {
        this.flags = flags;
    }

    @Before
    public void init() throws IOException {
        // Generate a temporary output folder
        tmpOutputDir = Files.createTempDirectory("oak-script-test-");

        // Adds the call to the script with:
        //   - --verify flag to avoid running the actual benchmark
        //   - -o flag to write the output to the temporary folder
        args = new ArrayList<>(
            Arrays.asList("bash", "run.sh", "--verify", "-o", tmpOutputDir.toAbsolutePath().toString())
        );

        // Generate random arguments' values
        Map<String, List<String>> multiExpect = new HashMap<>();
        Random r = new Random();
        for (Map.Entry<String, String> i : PARAMETERS.entrySet()) {
            // We test that the script runs the cartesian product of the summary parameters.
            int n = SUMMARY_PARAMETERS.contains(i.getValue()) ? 2 : 1;
            List<String> v = nextNumbers(r, n);
            args.add(i.getKey());
            args.add(String.join(" ", v));
            multiExpect.put(i.getValue(), v);
        }

        args.addAll(flags);
        this.expect = cartesianProduct(multiExpect);
        assert this.expect.size() == EXPECTED_SUMMARY_LINES;
    }

    private List<String> nextNumbers(Random r, int sz) {
        return r.ints(0, 1_000)
            .limit(sz)
            .mapToObj(Integer::toString)
            .collect(Collectors.toList());
    }

    @After
    public void tear() throws IOException {
        FileUtils.deleteDirectory(tmpOutputDir.toFile());
    }

    @Test
    public void wrongParameterTest() throws IOException, InterruptedException {
        args.add("--bad-arg");
        Out out = run();

        // Script should not have exit-code of 0
        Assert.assertNotEquals(out.out, 0, out.retCode);

        // No log files should be created
        File[] logs = getLogFiles(out);
        Assert.assertEquals(out.out, 0, logs.length);

        // The summary file should not be created
        File[] summary = getSummaryFiles(out);
        Assert.assertEquals(out.out, 0, summary.length);
    }

    @Test
    public void allParameterTest() throws IOException, InterruptedException {
        Out out = run();

        // Script should have exit-code of 0
        Assert.assertEquals(out.out, 0, out.retCode);

        // Validate number of rows in the summary file
        List<Map<String, String>> summary = getSummaryCsv(out);
        Assert.assertEquals(summary.toString(), EXPECTED_SUMMARY_LINES, summary.size());

        // Validate number of logs
        Map<String, List<String>> logs = getLogs(out);
        Assert.assertEquals(logs.toString(), EXPECTED_SUMMARY_LINES, logs.size());

        for (Map<String, String> params : expect) {
            // Make sure combination of parameters have a summary entry
            Map<String, String> summaryEntry = findSummaryEntry(summary, params);
            Assert.assertNotNull(summary.toString(), summaryEntry);

            // Verify creation of log file
            String logFileName = summaryEntry.get("log_file");
            List<String> logLines = logs.getOrDefault(logFileName, null);
            Assert.assertNotNull(String.format("Cannot find log: %s", logFileName), logLines);

            // Make sure all the parameters match
            for (Map.Entry<String, String> i : params.entrySet()) {
                assertMatch(logLines, i.getKey(), i.getValue());
            }

            // Verify flags are included/excluded
            for (String f : FLAGS) {
                if (flags.contains(f)) {
                    assertFlag(logLines, f);
                } else {
                    assertNoFlag(logLines, f);
                }
            }
        }
    }

    // Pre-defined known parameters and flags

    static final Map<String, String> PARAMETERS = new HashMap<>();
    static final List<String> FLAGS = Arrays.asList(
        "--consume-keys", "--consume-values", "--latency"
    );
    static final List<String> SUMMARY_PARAMETERS = Arrays.asList(
        "scenario", "bench", "heap_limit", "direct_limit", "threads", "gc_alg"
    );
    static final int EXPECTED_SUMMARY_LINES = (int) Math.pow(2, SUMMARY_PARAMETERS.size());

    static {
        // -o/--output and -v/--verify are set by default to facilitate the test
        PARAMETERS.put("-j", "java");
        PARAMETERS.put("-d", "duration");
        PARAMETERS.put("-i", "iterations");
        PARAMETERS.put("-w", "warmup");
        PARAMETERS.put("-h", "heap_limit");
        PARAMETERS.put("-l", "direct_limit");
        PARAMETERS.put("-r", "range_ratio");
        PARAMETERS.put("-s", "size");
        PARAMETERS.put("-t", "threads");
        PARAMETERS.put("-e", "scenario");
        PARAMETERS.put("-b", "bench");
        PARAMETERS.put("-g", "gc_alg");
        PARAMETERS.put("-m", "java_mode");
        PARAMETERS.put("--key", "key_class");
        PARAMETERS.put("--value", "value_class");
        PARAMETERS.put("--key-size", "key_size");
        PARAMETERS.put("--value-size", "value_size");
        PARAMETERS.put("--fill-threads", "fill_threads");
    }

    // Helper functions

    static class Out {
        String out;
        int retCode;
    }

    private Out run() throws IOException, InterruptedException {
        Process p = new ProcessBuilder(args).redirectErrorStream(true).start();
        Out out = new Out();

        BufferedReader buf = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder b = new StringBuilder();
        String line;
        while ((line = buf.readLine()) != null) {
            b.append(line);
            b.append("\n");
        }
        p.waitFor();
        out.out = b.toString();
        out.retCode = p.exitValue();
        return out;
    }

    /**
     * Generates all the permutations of the parameters.
     */
    public static List<Map<String, String>> cartesianProduct(Map<String, List<String>> c) {
        final ArrayList<Map<String, String>> res = new ArrayList<>();
        final int curLength = c.size();

        if (curLength == 0) {
            res.add(new HashMap<>());
            return res;
        }

        Optional<Map.Entry<String, List<String>>> firstEntry = c.entrySet().stream().findFirst();
        Map.Entry<String, List<String>> curEntry = firstEntry.get();

        Map<String, List<String>> subset = new HashMap<>(c);
        subset.remove(curEntry.getKey());

        for (Map<String, String> map : cartesianProduct(subset)) {
            for (String value : curEntry.getValue()) {
                Map<String, String> newMap = new HashMap<>(map);
                newMap.put(curEntry.getKey(), value);
                res.add(newMap);
            }
        }

        return res;
    }

    private File[] getSummaryFiles(Out out) {
        File[] files = tmpOutputDir.toFile().listFiles(((dir, name) -> name.endsWith(".csv")));
        Assert.assertNotNull(out.out, files);
        return files;
    }

    private List<Map<String, String>> getSummaryCsv(Out out) throws IOException {
        File[] files = getSummaryFiles(out);
        Assert.assertEquals(out.out, 1, files.length);

        List<String> lines = Files.readAllLines(files[0].toPath(), StandardCharsets.UTF_8);
        String[] header = null;
        List<Map<String, String>> records = new ArrayList<>();
        for (String line : lines) {
            if (line.length() == 0) {
                continue;
            }
            String[] columns = line.split(",");
            if (header == null) {
                header = columns;
                continue;
            }

            Map<String, String> record = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                record.put(header[i], columns[i]);
            }
            records.add(record);
        }

        return records;
    }


    private boolean matchSummaryEntry(
        Map<String, String> summaryEntry,
        Map<String, String> params
    ) {
        for (String key : SUMMARY_PARAMETERS) {
            if (!Objects.equals(summaryEntry.get(key), params.get(key))) {
                return false;
            }
        }

        return true;
    }

    private Map<String, String> findSummaryEntry(
        List<Map<String, String>> summary,
        Map<String, String> params
    ) {
        for (Map<String, String> summaryEntry : summary) {
            if (matchSummaryEntry(summaryEntry, params)) {
                return summaryEntry;
            }
        }

        return null;
    }

    private File[] getLogFiles(Out out) {
        File[] logs = tmpOutputDir.toFile().listFiles(((dir, name) -> name.endsWith(".log")));
        Assert.assertNotNull(out.out, logs);
        return logs;
    }

    private Map<String, List<String>> getLogs(Out out) throws IOException {
        File[] logs = getLogFiles(out);
        Map<String, List<String>> ret = new HashMap<>();
        for (File log : logs) {
            ret.put(log.getName(), Files.readAllLines(log.toPath(), StandardCharsets.UTF_8));
        }
        return ret;
    }

    private String getParamValue(List<String> lines, String key) {
        String keyPrefix = String.format("%s:", key);

        for (String line : lines) {
            if (!line.startsWith(keyPrefix)) {
                continue;
            }

            return line.substring(keyPrefix.length()).trim();
        }

        return null;
    }

    private void assertMatch(List<String> lines, String key, String expectedValue) {
        String actualValue = getParamValue(lines, key);
        Assert.assertNotNull(String.format("Did not find key: %s", key), actualValue);
        Assert.assertEquals(String.format("Value for key '%s' did not match.", key), expectedValue, actualValue);
    }

    private String getFlags(List<String> lines) {
        String key = "flag_arguments";
        String value = getParamValue(lines, key);
        Assert.assertNotNull(String.format("Did not find key: %s", key), value);
        return value;
    }

    private void assertNoFlag(List<String> lines, String expectedFlag) {
        String value = getFlags(lines);
        Assert.assertFalse(String.format("Flag '%s' was enabled", expectedFlag), value.contains(expectedFlag));
    }

    private void assertFlag(List<String> lines, String expectedFlag) {
        String value = getFlags(lines);
        Assert.assertTrue(String.format("Flag '%s' was not enabled", expectedFlag), value.contains(expectedFlag));
    }
}
