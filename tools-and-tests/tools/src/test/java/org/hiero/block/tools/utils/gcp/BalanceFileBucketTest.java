// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BalanceFileBucket}.
 *
 * <p>Tests timestamp formatting and parsing logic without requiring actual GCP access.
 */
class BalanceFileBucketTest {

    @Nested
    @DisplayName("Timestamp Formatting Tests")
    class TimestampFormattingTests {

        @Test
        @DisplayName("Format timestamp with full nanoseconds")
        void formatTimestampFullNanos() throws Exception {
            Instant timestamp = Instant.parse("2023-10-02T16:45:00.097179000Z");
            String formatted = invokeFormatTimestamp(timestamp);
            assertEquals("2023-10-02T16_45_00.097179000Z", formatted);
        }

        @Test
        @DisplayName("Format timestamp with trailing zeros in nanos")
        void formatTimestampTrailingZeros() throws Exception {
            Instant timestamp = Instant.parse("2019-09-13T22:00:00.000081000Z");
            String formatted = invokeFormatTimestamp(timestamp);
            assertEquals("2019-09-13T22_00_00.000081000Z", formatted);
        }

        @Test
        @DisplayName("Format timestamp at exact second")
        void formatTimestampExactSecond() throws Exception {
            Instant timestamp = Instant.parse("2023-10-01T00:00:00Z");
            String formatted = invokeFormatTimestamp(timestamp);
            assertEquals("2023-10-01T00_00_00.000000000Z", formatted);
        }
    }

    @Nested
    @DisplayName("Timestamp Normalization Tests")
    class TimestampNormalizationTests {

        @Test
        @DisplayName("Normalize timestamp with 6-digit nanos")
        void normalize6DigitNanos() throws Exception {
            String input = "2019-09-13T22_00_00.000081Z";
            String normalized = invokeNormalizeTimestamp(input);
            assertEquals("2019-09-13T22:00:00.000081000Z", normalized);
        }

        @Test
        @DisplayName("Normalize timestamp with 9-digit nanos")
        void normalize9DigitNanos() throws Exception {
            String input = "2023-10-02T16_45_00.097179000Z";
            String normalized = invokeNormalizeTimestamp(input);
            assertEquals("2023-10-02T16:45:00.097179000Z", normalized);
        }

        @Test
        @DisplayName("Normalize timestamp with 3-digit nanos")
        void normalize3DigitNanos() throws Exception {
            String input = "2023-01-01T12_30_45.123Z";
            String normalized = invokeNormalizeTimestamp(input);
            assertEquals("2023-01-01T12:30:45.123000000Z", normalized);
        }

        @Test
        @DisplayName("Normalized timestamp is parseable as Instant")
        void normalizedTimestampIsParseable() throws Exception {
            String input = "2019-09-13T22_00_00.000081Z";
            String normalized = invokeNormalizeTimestamp(input);
            Instant parsed = Instant.parse(normalized);
            assertEquals(81000, parsed.getNano());
        }
    }

    @Nested
    @DisplayName("Round-trip Tests")
    class RoundTripTests {

        @Test
        @DisplayName("Format then normalize produces parseable timestamp")
        void formatThenNormalize() throws Exception {
            Instant original = Instant.parse("2023-10-02T16:45:00.097179Z");
            String formatted = invokeFormatTimestamp(original);
            String normalized = invokeNormalizeTimestamp(formatted);
            Instant parsed = Instant.parse(normalized);

            // Note: Instant.parse truncates to 6 digits, so we compare at microsecond precision
            assertEquals(original.getEpochSecond(), parsed.getEpochSecond());
            assertEquals(original.getNano() / 1000, parsed.getNano() / 1000);
        }
    }

    /**
     * Invokes the private formatTimestamp method via reflection.
     */
    private String invokeFormatTimestamp(Instant timestamp) throws Exception {
        BalanceFileBucket bucket = new BalanceFileBucket(false, Path.of("/tmp"), 3, 34, null);
        Method method = BalanceFileBucket.class.getDeclaredMethod("formatTimestamp", Instant.class);
        method.setAccessible(true);
        return (String) method.invoke(bucket, timestamp);
    }

    /**
     * Invokes the private normalizeTimestamp method via reflection.
     */
    private String invokeNormalizeTimestamp(String timestampStr) throws Exception {
        BalanceFileBucket bucket = new BalanceFileBucket(false, Path.of("/tmp"), 3, 34, null);
        Method method = BalanceFileBucket.class.getDeclaredMethod("normalizeTimestamp", String.class);
        method.setAccessible(true);
        return (String) method.invoke(bucket, timestampStr);
    }
}
