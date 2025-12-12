// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for {@link DateUtil}.
 */
@DisplayName("DateUtil Tests")
public class DateUtilTest {

    @Nested
    @DisplayName("parseHumanDuration Tests")
    class ParseHumanDurationTests {

        @Test
        @DisplayName("Should parse milliseconds")
        void testParseMilliseconds() {
            Duration result = DateUtil.parseHumanDuration("100ms");
            assertEquals(Duration.ofMillis(100), result);
        }

        @Test
        @DisplayName("Should parse seconds")
        void testParseSeconds() {
            Duration result = DateUtil.parseHumanDuration("60s");
            assertEquals(Duration.ofSeconds(60), result);
        }

        @Test
        @DisplayName("Should parse minutes")
        void testParseMinutes() {
            Duration result = DateUtil.parseHumanDuration("2m");
            assertEquals(Duration.ofMinutes(2), result);
        }

        @Test
        @DisplayName("Should parse hours")
        void testParseHours() {
            Duration result = DateUtil.parseHumanDuration("1h");
            assertEquals(Duration.ofHours(1), result);
        }

        @Test
        @DisplayName("Should parse ISO-8601 duration")
        void testParseIso8601() {
            Duration result = DateUtil.parseHumanDuration("PT1M");
            assertEquals(Duration.ofMinutes(1), result);
        }

        @Test
        @DisplayName("Should parse ISO-8601 complex duration")
        void testParseIso8601Complex() {
            Duration result = DateUtil.parseHumanDuration("PT1H30M");
            assertEquals(Duration.ofMinutes(90), result);
        }

        @Test
        @DisplayName("Should throw exception for invalid format")
        void testParseInvalidFormat() {
            assertThrows(CommandLine.ParameterException.class, () -> {
                DateUtil.parseHumanDuration("invalid");
            });
        }

        @Test
        @DisplayName("Should throw exception for empty string")
        void testParseEmptyString() {
            assertThrows(CommandLine.ParameterException.class, () -> {
                DateUtil.parseHumanDuration("");
            });
        }

        @Test
        @DisplayName("Should parse zero duration")
        void testParseZeroDuration() {
            Duration result = DateUtil.parseHumanDuration("0s");
            assertEquals(Duration.ZERO, result);
        }

        @Test
        @DisplayName("Should parse large values")
        void testParseLargeValue() {
            Duration result = DateUtil.parseHumanDuration("1000h");
            assertEquals(Duration.ofHours(1000), result);
        }
    }

    @Nested
    @DisplayName("parseMirrorTimestamp Tests")
    class ParseMirrorTimestampTests {

        @Test
        @DisplayName("Should parse full timestamp with nanoseconds")
        void testParseFullTimestamp() {
            String timestamp = "1762898218.515837000";
            Instant result = DateUtil.parseMirrorTimestamp(timestamp);

            assertNotNull(result);
            assertEquals(1762898218L, result.getEpochSecond());
            assertEquals(515837000, result.getNano());
        }

        @Test
        @DisplayName("Should parse timestamp without fractional seconds")
        void testParseTimestampNoFraction() {
            String timestamp = "1762898218";
            Instant result = DateUtil.parseMirrorTimestamp(timestamp);

            assertNotNull(result);
            assertEquals(1762898218L, result.getEpochSecond());
            assertEquals(0, result.getNano());
        }

        @Test
        @DisplayName("Should pad short nanosecond strings")
        void testParsePadShortNanos() {
            String timestamp = "1762898218.5";
            Instant result = DateUtil.parseMirrorTimestamp(timestamp);

            assertNotNull(result);
            assertEquals(1762898218L, result.getEpochSecond());
            assertEquals(500000000, result.getNano()); // "5" padded to 9 digits = 500000000
        }

        @Test
        @DisplayName("Should truncate long nanosecond strings")
        void testParseTruncateLongNanos() {
            String timestamp = "1762898218.1234567890"; // 10 digits
            Instant result = DateUtil.parseMirrorTimestamp(timestamp);

            assertNotNull(result);
            assertEquals(1762898218L, result.getEpochSecond());
            assertEquals(123456789, result.getNano()); // Truncated to 9 digits
        }

        @Test
        @DisplayName("Should return null for null input")
        void testParseNullInput() {
            Instant result = DateUtil.parseMirrorTimestamp(null);
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null for empty input")
        void testParseEmptyInput() {
            Instant result = DateUtil.parseMirrorTimestamp("");
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null for malformed input")
        void testParseMalformedInput() {
            Instant result = DateUtil.parseMirrorTimestamp("not-a-timestamp");
            assertNull(result);
        }

        @Test
        @DisplayName("Should handle zero timestamp")
        void testParseZeroTimestamp() {
            String timestamp = "0.0";
            Instant result = DateUtil.parseMirrorTimestamp(timestamp);

            assertNotNull(result);
            assertEquals(Instant.EPOCH, result);
        }

        @Test
        @DisplayName("Should handle timestamp with leading zeros in nanos")
        void testParseLeadingZerosInNanos() {
            String timestamp = "1762898218.000000123";
            Instant result = DateUtil.parseMirrorTimestamp(timestamp);

            assertNotNull(result);
            assertEquals(1762898218L, result.getEpochSecond());
            assertEquals(123, result.getNano());
        }

        @Test
        @DisplayName("Should handle very large epoch seconds")
        void testParseLargeEpochSeconds() {
            String timestamp = "9999999999.123456789";
            Instant result = DateUtil.parseMirrorTimestamp(timestamp);

            assertNotNull(result);
            assertEquals(9999999999L, result.getEpochSecond());
            assertEquals(123456789, result.getNano());
        }
    }
}
