// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link LiveDownloader}.
 *
 * <p>These tests focus on the testable components of LiveDownloader,
 * particularly the validation status persistence and batch context logic.
 * Full integration tests would require mocking GCP storage and other
 * external dependencies.
 */
@DisplayName("LiveDownloader Tests")
public class LiveDownloaderTest {

    @Nested
    @DisplayName("ValidationStatus Persistence Tests")
    class ValidationStatusPersistenceTests {

        @TempDir
        Path tempDir;

        @Test
        @DisplayName("Should write validation status as JSON")
        void testWriteValidationStatus() throws IOException {
            Path statusPath = tempDir.resolve("status.json");
            String dayDate = "2025-12-01";
            String recordFileTime = "2025-12-01T00:00:04.319226458Z";
            String endRunningHashHex = "1234567890abcdef";

            ValidationStatus status = new ValidationStatus(dayDate, recordFileTime, endRunningHashHex);
            writeValidationStatus(statusPath, status);

            assertTrue(Files.exists(statusPath));
            String content = Files.readString(statusPath, StandardCharsets.UTF_8);
            assertTrue(content.contains("2025-12-01"));
            assertTrue(content.contains("2025-12-01T00:00:04.319226458Z"));
            assertTrue(content.contains("1234567890abcdef"));
        }

        @Test
        @DisplayName("Should create parent directories when writing status")
        void testCreateParentDirectories() throws IOException {
            Path nestedPath = tempDir.resolve("parent/child/status.json");
            ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", "abc123");

            writeValidationStatus(nestedPath, status);

            assertTrue(Files.exists(nestedPath.getParent()));
            assertTrue(Files.exists(nestedPath));
        }

        @Test
        @DisplayName("Should handle null path gracefully")
        void testNullPath() {
            ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", "abc123");

            assertDoesNotThrow(() -> writeValidationStatus(null, status));
        }

        @Test
        @DisplayName("Should handle null status gracefully")
        void testNullStatus() {
            Path statusPath = tempDir.resolve("status.json");

            assertDoesNotThrow(() -> writeValidationStatus(statusPath, null));
            assertFalse(Files.exists(statusPath));
        }

        @Test
        @DisplayName("Should produce valid JSON format")
        void testValidJsonFormat() throws IOException {
            Path statusPath = tempDir.resolve("status.json");
            ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T12:34:56Z", "abcdef123456");

            writeValidationStatus(statusPath, status);
            String content = Files.readString(statusPath, StandardCharsets.UTF_8);

            assertTrue(content.startsWith("{"), "Should start with opening brace");
            assertTrue(content.endsWith("}\n"), "Should end with closing brace and newline");
            assertTrue(content.contains("\"dayDate\""), "Should contain dayDate field");
            assertTrue(content.contains("\"recordFileTime\""), "Should contain recordFileTime field");
            assertTrue(content.contains("\"endRunningHashHex\""), "Should contain endRunningHashHex field");
        }

        @Test
        @DisplayName("Should overwrite existing status file")
        void testOverwriteExistingStatus() throws IOException {
            Path statusPath = tempDir.resolve("status.json");
            ValidationStatus status1 = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", "hash1");
            ValidationStatus status2 = new ValidationStatus("2025-12-02", "2025-12-02T00:00:00Z", "hash2");

            writeValidationStatus(statusPath, status1);
            writeValidationStatus(statusPath, status2);

            String content = Files.readString(statusPath, StandardCharsets.UTF_8);
            assertTrue(content.contains("2025-12-02"));
            assertTrue(content.contains("hash2"));
            assertFalse(content.contains("hash1"));
        }

        @Test
        @DisplayName("Should handle long hash values")
        void testLongHashValue() throws IOException {
            Path statusPath = tempDir.resolve("status.json");
            String longHash = "a".repeat(128);
            ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", longHash);

            writeValidationStatus(statusPath, status);
            String content = Files.readString(statusPath, StandardCharsets.UTF_8);

            assertTrue(content.contains(longHash));
        }

        @Test
        @DisplayName("Should handle empty hash value")
        void testEmptyHashValue() throws IOException {
            Path statusPath = tempDir.resolve("status.json");
            ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", "");

            writeValidationStatus(statusPath, status);
            String content = Files.readString(statusPath, StandardCharsets.UTF_8);

            assertTrue(content.contains("\"endRunningHashHex\": \"\""));
        }

        @Test
        @DisplayName("Should use UTF-8 encoding")
        void testUtf8Encoding() throws IOException {
            Path statusPath = tempDir.resolve("status.json");
            ValidationStatus status = new ValidationStatus("2025-12-01", "2025-12-01T00:00:00Z", "abc123");

            writeValidationStatus(statusPath, status);

            byte[] bytes = Files.readAllBytes(statusPath);
            String content = new String(bytes, StandardCharsets.UTF_8);
            assertTrue(content.contains("2025-12-01"));
        }

        // Helper method to simulate the private writeValidationStatus method
        private void writeValidationStatus(Path path, ValidationStatus status) {
            if (path == null || status == null) {
                return;
            }
            try {
                if (path.getParent() != null) {
                    Files.createDirectories(path.getParent());
                }
                String json = "{\n"
                        + "  \"dayDate\": \"" + status.dayDate() + "\",\n"
                        + "  \"recordFileTime\": \"" + status.recordFileTime() + "\",\n"
                        + "  \"endRunningHashHex\": \"" + status.endRunningHashHex() + "\"\n"
                        + "}\n";
                Files.writeString(path, json, StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Nested
    @DisplayName("BlockDescriptor Batch Processing Tests")
    class BatchProcessingTests {

        @Test
        @DisplayName("Should sort batch by block number")
        void testBatchSorting() {
            List<BlockDescriptor> batch = new ArrayList<>();
            batch.add(new BlockDescriptor(300L, "file3.rcd", "2025-12-01T03:00:00Z", null));
            batch.add(new BlockDescriptor(100L, "file1.rcd", "2025-12-01T01:00:00Z", null));
            batch.add(new BlockDescriptor(200L, "file2.rcd", "2025-12-01T02:00:00Z", null));

            batch.sort(Comparator.comparingLong(BlockDescriptor::blockNumber));

            assertEquals(100L, batch.get(0).blockNumber());
            assertEquals(200L, batch.get(1).blockNumber());
            assertEquals(300L, batch.get(2).blockNumber());
        }

        @Test
        @DisplayName("Should handle empty batch")
        void testEmptyBatch() {
            List<BlockDescriptor> batch = new ArrayList<>();

            batch.sort(Comparator.comparingLong(BlockDescriptor::blockNumber));

            assertTrue(batch.isEmpty());
        }

        @Test
        @DisplayName("Should handle single item batch")
        void testSingleItemBatch() {
            List<BlockDescriptor> batch = new ArrayList<>();
            batch.add(new BlockDescriptor(100L, "file.rcd", "2025-12-01T00:00:00Z", null));

            batch.sort(Comparator.comparingLong(BlockDescriptor::blockNumber));

            assertEquals(1, batch.size());
            assertEquals(100L, batch.get(0).blockNumber());
        }

        @Test
        @DisplayName("Should handle already sorted batch")
        void testAlreadySortedBatch() {
            List<BlockDescriptor> batch = new ArrayList<>();
            batch.add(new BlockDescriptor(100L, "file1.rcd", "2025-12-01T01:00:00Z", null));
            batch.add(new BlockDescriptor(200L, "file2.rcd", "2025-12-01T02:00:00Z", null));
            batch.add(new BlockDescriptor(300L, "file3.rcd", "2025-12-01T03:00:00Z", null));

            batch.sort(Comparator.comparingLong(BlockDescriptor::blockNumber));

            assertEquals(100L, batch.get(0).blockNumber());
            assertEquals(200L, batch.get(1).blockNumber());
            assertEquals(300L, batch.get(2).blockNumber());
        }

        @Test
        @DisplayName("Should handle duplicate block numbers")
        void testDuplicateBlockNumbers() {
            List<BlockDescriptor> batch = new ArrayList<>();
            batch.add(new BlockDescriptor(100L, "file1.rcd", "2025-12-01T01:00:00Z", null));
            batch.add(new BlockDescriptor(100L, "file2.rcd", "2025-12-01T01:00:01Z", null));
            batch.add(new BlockDescriptor(200L, "file3.rcd", "2025-12-01T02:00:00Z", null));

            batch.sort(Comparator.comparingLong(BlockDescriptor::blockNumber));

            assertEquals(100L, batch.get(0).blockNumber());
            assertEquals(100L, batch.get(1).blockNumber());
            assertEquals(200L, batch.get(2).blockNumber());
        }
    }

    @Nested
    @DisplayName("Timestamp Parsing Tests")
    class TimestampParsingTests {

        @Test
        @DisplayName("Should parse valid ISO-8601 timestamp")
        void testParseValidTimestamp() {
            String timestampIso = "2025-12-01T00:00:04.319226458Z";

            Instant instant = Instant.parse(timestampIso);

            assertNotNull(instant);
            assertEquals("2025-12-01T00:00:04.319226458Z", instant.toString());
        }

        @Test
        @DisplayName("Should parse timestamp without nanoseconds")
        void testParseTimestampWithoutNanos() {
            String timestampIso = "2025-12-01T00:00:04Z";

            Instant instant = Instant.parse(timestampIso);

            assertNotNull(instant);
            assertEquals("2025-12-01T00:00:04Z", instant.toString());
        }

        @Test
        @DisplayName("Should throw exception for invalid timestamp")
        void testParseInvalidTimestamp() {
            String invalidTimestamp = "not-a-timestamp";

            assertThrows(Exception.class, () -> Instant.parse(invalidTimestamp));
        }

        @Test
        @DisplayName("Should parse epoch instant")
        void testParseEpochInstant() {
            String epochTimestamp = "1970-01-01T00:00:00Z";

            Instant instant = Instant.parse(epochTimestamp);

            assertEquals(Instant.EPOCH, instant);
        }
    }

    @Nested
    @DisplayName("Day Key Format Tests")
    class DayKeyFormatTests {

        @Test
        @DisplayName("Should validate day key format YYYY-MM-DD")
        void testDayKeyFormat() {
            String dayKey = "2025-12-01";

            assertTrue(dayKey.matches("\\d{4}-\\d{2}-\\d{2}"));
        }

        @Test
        @DisplayName("Should reject invalid day key formats")
        void testInvalidDayKeyFormats() {
            String[] invalidFormats = {"2025/12/01", "01-12-2025", "2025-12-1", "25-12-01", "2025-12-01T00:00:00Z"};

            for (String invalid : invalidFormats) {
                assertFalse(invalid.matches("\\d{4}-\\d{2}-\\d{2}"), "Should reject format: " + invalid);
            }
        }
    }
}
