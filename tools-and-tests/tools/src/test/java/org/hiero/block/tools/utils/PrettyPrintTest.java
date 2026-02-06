// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PrettyPrintTest {

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
}
