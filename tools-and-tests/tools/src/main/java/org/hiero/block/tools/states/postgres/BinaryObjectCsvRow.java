package org.hiero.block.tools.states.postgres;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * Represents a row in the binary_objects table. CVS columns "id,ref_count,hash_hex,file_oid,file_base64"
 *
 * @param id           from "id" column, parsed as long
 * @param refCount     from "ref_count" column, parsed as long
 * @param hash         from "hash_hex" column, parsed as byte[]
 * @param fileId       from "file_oid" column, parsed as int
 * @param fileContents from "file_base64" column, parsed as byte[]
 */
public record BinaryObjectCsvRow(
        long id,
        long refCount,
        byte[] hash,
        int fileId,
        byte[] fileContents
) {

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
     * Loads binary objects from a CSV file.
     *
     * @param csvPath the path to the CSV file containing binary objects
     * @return a list of BinaryObjectCsvRow objects representing the rows in the CSV file
     */
    public static List<BinaryObjectCsvRow> loadBinaryObjects(Path csvPath) {
        try (var lines = Files.lines(csvPath)) {
            return lines
                    .skip(1) // Skip header line
                    .map(line -> {
                        String[] parts = line.split(",");
                        if (parts.length < 3) {
                            return null; // Skip malformed lines
                        }
                        //CVS columns "id,ref_count,hash_hex,file_oid,file_base64"
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
                                fileContents = new byte[0]; // Handle empty or malformed file contents
                                e.printStackTrace();
                                System.err.println("BAD parts[4] = ["+parts[4]+"] length = " + parts[4].length());
                                // print each character in parts[4] as hex
                                for (int i = 0; i < parts[4].length(); i++) {
                                    char c = parts[4].charAt(i);
                                    System.err.printf("Character '%c' at index %d: %02X%n", c, i, (int) c);
                                }
                            }
                        }
                        return new BinaryObjectCsvRow(id, refCount, hash, fileId, fileContents);
                    })
                    .filter(row -> row != null)
                    .toList();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load binary objects from CSV", e);
        }
    }
}
