// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ValidationStatus}.
 */
@DisplayName("ValidationStatus Tests")
public class ValidationStatusTest {

    @Test
    @DisplayName("Should create ValidationStatus with all fields")
    void testConstructor() {
        String dayDate = "2025-12-01";
        String recordFileTime = "2025-12-01T00:00:04.319226458Z";
        String endRunningHashHex = "1234567890abcdef";

        ValidationStatus status = new ValidationStatus(dayDate, recordFileTime, endRunningHashHex);

        assertEquals(dayDate, status.dayDate());
        assertEquals(recordFileTime, status.recordFileTime());
        assertEquals(endRunningHashHex, status.endRunningHashHex());
    }

    @Test
    @DisplayName("Should handle hex hash without 0x prefix")
    void testHashWithoutPrefix() {
        String hash = "abcd1234ef567890";

        ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", hash);

        assertEquals(hash, status.endRunningHashHex());
        assertFalse(status.endRunningHashHex().startsWith("0x"));
    }

    @Test
    @DisplayName("Should handle hex hash with 0x prefix")
    void testHashWithPrefix() {
        String hash = "0xabcd1234ef567890";

        ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", hash);

        assertEquals(hash, status.endRunningHashHex());
        assertTrue(status.endRunningHashHex().startsWith("0x"));
    }

    @Test
    @DisplayName("Should preserve ISO-8601 timestamp format")
    void testTimestampFormat() {
        String timestamp = "2025-12-01T12:34:56.789123456Z";

        ValidationStatus status = new ValidationStatus("2025-12-01", timestamp, "abc123");

        assertEquals(timestamp, status.recordFileTime());
        assertTrue(status.recordFileTime().endsWith("Z"));
    }

    @Test
    @DisplayName("Should preserve day date format")
    void testDayDateFormat() {
        String dayDate = "2025-12-31";

        ValidationStatus status = new ValidationStatus(dayDate, "2025-12-31T23:59:59Z", "hash");

        assertEquals(dayDate, status.dayDate());
        assertTrue(status.dayDate().matches("\\d{4}-\\d{2}-\\d{2}"));
    }

    @Test
    @DisplayName("Should handle long hash strings")
    void testLongHashString() {
        String longHash = "a".repeat(128); // Very long hash

        ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", longHash);

        assertEquals(longHash, status.endRunningHashHex());
        assertEquals(128, status.endRunningHashHex().length());
    }

    @Test
    @DisplayName("Should handle empty hash string")
    void testEmptyHashString() {
        String emptyHash = "";

        ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", emptyHash);

        assertEquals("", status.endRunningHashHex());
    }

    @Test
    @DisplayName("Should be immutable")
    void testImmutability() {
        ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", "abc123");

        String dayDate1 = status.dayDate();
        String dayDate2 = status.dayDate();
        String time1 = status.recordFileTime();
        String time2 = status.recordFileTime();
        String hash1 = status.endRunningHashHex();
        String hash2 = status.endRunningHashHex();

        assertSame(dayDate1, dayDate2);
        assertSame(time1, time2);
        assertSame(hash1, hash2);
    }

    @Test
    @DisplayName("Should handle typical 48-byte hash (96 hex chars)")
    void testTypical48ByteHash() {
        String hash = "1234567890abcdef".repeat(6); // 96 chars

        ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", hash);

        assertEquals(96, status.endRunningHashHex().length());
    }
}
