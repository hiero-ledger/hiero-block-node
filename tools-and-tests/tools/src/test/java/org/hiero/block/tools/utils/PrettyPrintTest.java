// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PrettyPrintTest {

    // ===== simpleHash tests =====

    @Test
    void simpleHashReturnsFirst6HexCharsUpperCase() {
        byte[] hash = new byte[] {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF, 0x01, 0x23, 0x45, 0x67};
        assertEquals("ABCDEF", PrettyPrint.simpleHash(hash));
    }

    @Test
    void simpleHashWithAllZeros() {
        byte[] hash = new byte[48];
        assertEquals("000000", PrettyPrint.simpleHash(hash));
    }

    @Test
    void simpleHashWithMinimumThreeBytes() {
        // 3 bytes produce exactly 6 hex characters
        byte[] hash = new byte[] {(byte) 0xFF, (byte) 0x00, (byte) 0xAA};
        assertEquals("FF00AA", PrettyPrint.simpleHash(hash));
    }

    @Test
    void simpleHashTruncatesLongerHash() {
        // SHA-384 produces 48 bytes; only first 3 bytes (6 hex chars) should appear
        byte[] hash = new byte[48];
        hash[0] = (byte) 0xDE;
        hash[1] = (byte) 0xAD;
        hash[2] = (byte) 0xBE;
        hash[3] = (byte) 0xEF; // should not appear in output
        assertEquals("DEADBE", PrettyPrint.simpleHash(hash));
    }

    @Test
    void simpleHashWithLowNibbleValues() {
        // Verify zero-padding in hex (e.g. 0x0A -> "0a" -> "0A")
        byte[] hash = new byte[] {0x0A, 0x0B, 0x0C, 0x0D};
        assertEquals("0A0B0C", PrettyPrint.simpleHash(hash));
    }

    @Test
    void simpleHashThrowsOnTooShortArray() {
        // Only 2 bytes = 4 hex chars, substring(0,6) should throw
        byte[] hash = new byte[] {(byte) 0xAB, (byte) 0xCD};
        assertThrows(StringIndexOutOfBoundsException.class, () -> PrettyPrint.simpleHash(hash));
    }

    // ===== prettyPrintFileSize tests =====

    @Test
    void prettyPrintFileSize_bytes() {
        assertEquals("0 B", PrettyPrint.prettyPrintFileSize(0));
        assertEquals("1 B", PrettyPrint.prettyPrintFileSize(1));
        assertEquals("512 B", PrettyPrint.prettyPrintFileSize(512));
        assertEquals("1023 B", PrettyPrint.prettyPrintFileSize(1023));
    }

    @Test
    void prettyPrintFileSize_kilobytes() {
        assertEquals("1.0 KB", PrettyPrint.prettyPrintFileSize(1024));
        assertEquals("1.5 KB", PrettyPrint.prettyPrintFileSize(1536));
        assertEquals("10.0 KB", PrettyPrint.prettyPrintFileSize(10240));
    }

    @Test
    void prettyPrintFileSize_megabytes() {
        assertEquals("1.0 MB", PrettyPrint.prettyPrintFileSize(1024 * 1024));
        assertEquals("5.0 MB", PrettyPrint.prettyPrintFileSize(5 * 1024 * 1024));
    }

    @Test
    void prettyPrintFileSize_gigabytes() {
        assertEquals("1.0 GB", PrettyPrint.prettyPrintFileSize(1024L * 1024 * 1024));
        assertEquals("2.5 GB", PrettyPrint.prettyPrintFileSize((long) (2.5 * 1024 * 1024 * 1024)));
    }

    @Test
    void prettyPrintFileSize_terabytes() {
        assertEquals("1.0 TB", PrettyPrint.prettyPrintFileSize(1024L * 1024 * 1024 * 1024));
    }

    // ===== computeRemainingMilliseconds tests =====

    @Test
    void computeRemaining_halfwayDone() {
        // 50 of 100 done in 10000ms → 10000ms remaining
        assertEquals(10000, PrettyPrint.computeRemainingMilliseconds(50, 100, 10000));
    }

    @Test
    void computeRemaining_zeroProcessed_returnsMaxValue() {
        assertEquals(Long.MAX_VALUE, PrettyPrint.computeRemainingMilliseconds(0, 100, 5000));
    }

    @Test
    void computeRemaining_allDone_returnsZero() {
        assertEquals(0, PrettyPrint.computeRemainingMilliseconds(100, 100, 5000));
    }

    @Test
    void computeRemaining_quarterDone() {
        // 25 of 100 done in 5000ms → 15000ms remaining
        assertEquals(15000, PrettyPrint.computeRemainingMilliseconds(25, 100, 5000));
    }

    @Test
    void computeRemaining_overProcessed_returnsZero() {
        // More processed than total → negative remaining clamped to 0
        assertEquals(0, PrettyPrint.computeRemainingMilliseconds(150, 100, 5000));
    }

    @Test
    void computeRemaining_oneOfMillion() {
        // 1 of 1_000_000 done in 100ms → ~99_999_900ms remaining
        long result = PrettyPrint.computeRemainingMilliseconds(1, 1_000_000, 100);
        assertEquals(99_999_900L, result);
    }
}
