// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.balances;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Utility class to load balances from a CSV file.
 */
public class CsvAccountBalances {

    /**
     * Loads balances from a CSV file URL and returns a map where the key is the account ID, and the value is the balance.
     *
     * @return Map of account balances keyed by account ID.
     * @throws RuntimeException if an error occurs while reading the CSV file.
     */
    public static Map<Long, Long> loadCsvBalances(URL csvUrl) {
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(
                csvUrl.toString().endsWith(".gz") ? new GZIPInputStream(csvUrl.openStream()) : csvUrl.openStream(),
                StandardCharsets.UTF_8))) {
            return reader.lines().skip(2) // Skip date and header lines
                .map(line -> line.split(","))
                .collect(Collectors.toMap(
                    parts -> parseLong(parts[2]), // Account ID
                    parts -> parseLong(parts[3]) // Balance
                ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load balances from " + csvUrl, e);
        }
    }

    /**
     * Parses a string to a Long, handling any parsing errors gracefully.
     *
     * @param str the string to parse
     * @return the parsed Long value, or Long.MIN_VALUE if parsing fails
     */
    private static Long parseLong(String str) {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            System.err.println("Failed to parse long from string: " + str + ". Error: " + e.getMessage());
            return Long.MIN_VALUE; // or handle the error as needed
        }
    }
}
