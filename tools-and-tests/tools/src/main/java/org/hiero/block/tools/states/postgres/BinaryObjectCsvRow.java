// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Represents a row in the binary_objects table. CVS columns "id,ref_count,hash_hex,file_oid,file_base64"
 *
 * @param id           from "id" column, parsed as long
 * @param refCount     from "ref_count" column, parsed as long
 * @param hash         from "hash_hex" column, parsed as byte[]
 * @param fileId       from "file_oid" column, parsed as int
 * @param fileContents from "file_base64" column, parsed as byte[]
 */
@SuppressWarnings("unused")
public record BinaryObjectCsvRow(long id, long refCount, byte[] hash, int fileId, byte[] fileContents) {

    public String hexHash() {
        return HexFormat.of().formatHex(hash);
    }

    public String computeHexHash() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-384");
            byte[] hash = digest.digest(fileContents);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads binary objects from a CSV file at the given URL and returns them as a map keyed by their hex hash.
     *
     * @param csvUrl the URL to the CSV file containing binary objects
     * @return a map of hex hash strings to BinaryObjectCsvRow objects
     */
    public static Map<String, BinaryObjectCsvRow> loadBinaryObjectsMap(URL csvUrl) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                csvUrl.toString().endsWith(".gz") ? new GZIPInputStream(csvUrl.openStream()) : csvUrl.openStream(),
                StandardCharsets.UTF_8))) {
            return parseLineToRow(reader.lines())
                    .collect(Collectors.toMap(BinaryObjectCsvRow::hexHash, Function.identity()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Loads binary objects from a CSV file and returns them as a map keyed by their hex hash.
     *
     * @param csvPath the path to the CSV file containing binary objects
     * @return a map of hex hash strings to BinaryObjectCsvRow objects
     */
    public static Map<String, BinaryObjectCsvRow> loadBinaryObjectsMap(Path csvPath) {
        try (BufferedReader reader = newReaderForPath(csvPath)) {
            return parseLineToRow(reader.lines())
                    .collect(Collectors.toMap(BinaryObjectCsvRow::hexHash, Function.identity()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load binary objects from CSV", e);
        }
    }

    /**
     * Loads binary objects from a CSV file.
     *
     * @param csvPath the path to the CSV file containing binary objects
     * @return a list of BinaryObjectCsvRow objects representing the rows in the CSV file
     */
    public static List<BinaryObjectCsvRow> loadBinaryObjects(Path csvPath) {
        try (BufferedReader reader = newReaderForPath(csvPath)) {
            return parseLineToRow(reader.lines()).toList();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load binary objects from CSV", e);
        }
    }

    /**
     * Creates a BufferedReader for the given path, automatically decompressing if the file ends with .gz.
     */
    private static BufferedReader newReaderForPath(Path csvPath) throws IOException {
        if (csvPath.toString().endsWith(".gz")) {
            return new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new BufferedInputStream(Files.newInputStream(csvPath))),
                    StandardCharsets.UTF_8));
        }
        return Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
    }

    /**
     * Parses lines of a CSV file into a stream of BinaryObjectCsvRow objects.
     *
     * @param lines the stream of lines from the CSV file
     * @return a stream of BinaryObjectCsvRow objects
     */
    private static Stream<BinaryObjectCsvRow> parseLineToRow(Stream<String> lines) {
        return lines.skip(1) // Skip header line
                .map(line -> {
                    String[] parts = line.split(",");
                    if (parts.length < 3) {
                        return null; // Skip malformed lines
                    }
                    // CVS columns "id,ref_count,hash_hex,file_oid,file_base64"
                    long id = Long.parseLong(parts[0]);
                    long refCount = Long.parseLong(parts[1]);
                    byte[] hash = HexFormat.of().parseHex(parts[2]);
                    int fileId = Integer.parseInt(parts[3]);
                    byte[] fileContents;
                    if (parts[4].isEmpty() || parts[4].equals("\"\"")) {
                        fileContents = new byte[0]; // Handle empty file contents
                    } else {
                        try {
                            fileContents = HexFormat.of().parseHex(parts[4]);
                        } catch (NumberFormatException e) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("Failed to parse file contents for id ")
                                    .append(id)
                                    .append(", BAD parts[4] = [")
                                    .append(parts[4])
                                    .append("] length = ")
                                    .append(parts[4].length());
                            // print each character in parts[4] as hex
                            for (int i = 0; i < parts[4].length(); i++) {
                                char c = parts[4].charAt(i);
                                sb.append(String.format("Character '%c' at index %d: %02X%n", c, i, (int) c));
                            }
                            throw new RuntimeException(sb.toString(), e);
                        }
                    }
                    return new BinaryObjectCsvRow(id, refCount, hash, fileId, fileContents);
                })
                .filter(Objects::nonNull);
    }
}
