/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PrintTools {

    public static String dashLine(int sz) {
        return Stream.generate(() -> "-").limit(sz).collect(Collectors.joining());
    }

    // Print a header message on the standard output
    public static void printHeader(String header, Object... args) {
        String headerRow = String.format(header, args);
        String line = dashLine(header.length());
        System.out.println(line);
        System.out.println(headerRow);
        System.out.println(line);
    }
}
