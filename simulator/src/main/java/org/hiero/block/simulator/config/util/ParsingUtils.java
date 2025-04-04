// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.util;

import static java.util.Objects.requireNonNull;

import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParsingUtils {
    public static LinkedHashSet<Long> parseRangeSet(String input) {
        requireNonNull(input);
        final LinkedHashSet<Long> result = new LinkedHashSet<>();
        if (!input.isBlank()) {
            input = input.trim();
            final Pattern pattern = Pattern.compile("^(\\[(\\d+)-(\\d+)\\]|\\d+)(,\\s*(\\[(\\d+)-(\\d+)\\]|\\d+))*$");
            if (!pattern.matcher(input).matches()) {
                throw new IllegalArgumentException(input + " does not match the expected format " + pattern.pattern());
            }
            try {
                final Matcher matcher =
                        Pattern.compile("\\[(\\d+)-(\\d+)\\]|(\\d+)").matcher(input);
                while (matcher.find()) {
                    if (matcher.group(1) != null && matcher.group(2) != null) {
                        final long start = Long.parseLong(matcher.group(1));
                        final long end = Long.parseLong(matcher.group(2));
                        if (start <= 0 || end <= 0) {
                            throw new IllegalArgumentException(input
                                    + " does not match the expected format. Range values must be positive and non-zero: ["
                                    + start + "-" + end + "]");
                        }
                        if (start >= end) {
                            throw new IllegalArgumentException(
                                    input + " does not match the expected format. Invalid range: start >= end in ["
                                            + start + "-" + end + "]");
                        }
                        for (long i = start; i <= end; i++) {
                            result.add(i);
                        }
                    } else if (matcher.group(3) != null) {
                        final long value = Long.parseLong(matcher.group(3));
                        if (value <= 0) {
                            throw new IllegalArgumentException("Values must be positive and non-zero: " + value);
                        }
                        result.add(value);
                    }
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Exception in parsing input: " + input, e);
            }
        }
        return result;
    }
}
