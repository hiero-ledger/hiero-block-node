package org.hiero.block.tools.states.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class to load balances from a CSV file.
 */
public class CsvBalances {

    /** Holds balances from the CSV file for the date 2019-09-13T22:00:00.000081Z */
    public static final Map<Long,Long> BALANCES_CSV_2019_09_13T22 =
            loadCsvBalances(Path.of("mainnet-data/2019-09-13T22_00_00.000081Z_Balances.csv"));

    /**
     * Loads balances from a CSV file and returns a map where the key is the account ID, and the value is the balance.
     *
     * @return Map of account balances keyed by account ID.
     * @throws RuntimeException if an error occurs while reading the CSV file.
     */
    public static Map<Long,Long> loadCsvBalances(Path csvFilePath) {
        try(Stream<String> lines = Files.lines(csvFilePath)) {
            // skip the date and header lines and parse the rest
            return lines.skip(2) // Skip date and header lines
                    .map(line -> line.split(","))
                    .collect(Collectors.toMap(
                            parts -> parseLong(parts[2]), // Account ID
                            parts -> parseLong(parts[3])  // Balance
                    ));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load balances from " + csvFilePath, e);
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
