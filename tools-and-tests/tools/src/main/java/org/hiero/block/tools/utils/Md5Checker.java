// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Utility class for checking MD5 checksums.
 */
public class Md5Checker {
    /**
     * Check if the MD5 checksum of the file at filePath matches the expected MD5 checksum.
     *
     * @param expectedMd5Hex the expected MD5 checksum
     * @param filePath the path to the file to check
     * @return true if the MD5 checksum matches, false otherwise
     * @throws IOException if an I/O error occurs
     */
    public static boolean checkMd5(String expectedMd5Hex, Path filePath) throws IOException {
        // compute the md5Hex of the file at filePath by calling command line app "md5sum <filePath>"
        ProcessBuilder pb = new ProcessBuilder("md5sum", filePath.toString());
        try {
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("md5sum command failed with exit code " + exitCode);
            }
            byte[] output = process.getInputStream().readAllBytes();
            String computedMd5Hex = new String(output).split(" ")[0];
            return expectedMd5Hex.equals(computedMd5Hex);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("md5sum command was interrupted", e);
        }
    }

    /**
     * Check if the MD5 checksum of the given data matches the expected MD5 checksum.
     *
     * @param expectedMd5Hex the expected MD5 checksum
     * @param data the data to check
     * @return true if the MD5 checksum matches, false otherwise
     * @throws Exception if an error occurs while computing the MD5 checksum
     */
    public static boolean checkMd5(String expectedMd5Hex, byte[] data) throws Exception {
        return expectedMd5Hex.equals(computeMd5Hex(data));
    }

    /**
     * Compute the MD5 checksum of the given data and return it as a hex string.
     * Note: MD5 is used for GCS compatibility (file verification), not for security purposes.
     *
     * @param data the data to compute the checksum for
     * @return the MD5 checksum as a lowercase hex string
     * @throws Exception if an error occurs while computing the MD5 checksum
     */
    @SuppressWarnings("java:S4790") // MD5 is required for GCS compatibility, not for security
    public static String computeMd5Hex(byte[] data) throws Exception {
        // nosemgrep: java.lang.security.audit.crypto.weak-hash.use-of-md5
        MessageDigest md = MessageDigest.getInstance("MD5");
        return HexFormat.of().formatHex(md.digest(data));
    }
}
