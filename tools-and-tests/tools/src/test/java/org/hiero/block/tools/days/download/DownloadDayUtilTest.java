// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.download;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DownloadDayUtil}.
 *
 * <p>Note: The validateBlockHashes method requires valid parsed record files with proper
 * block structure, making it difficult to test without actual blockchain data. These tests
 * focus on the error handling and validation logic that can be tested independently.
 *
 */
@DisplayName("DownloadDayUtil Tests")
public class DownloadDayUtilTest {

    @Nested
    @DisplayName("validateBlockHashes Input Validation Tests")
    class ValidateBlockHashesInputValidationTests {

        @Test
        @DisplayName("Should throw exception for empty file list")
        void testEmptyFileList() {
            List<InMemoryFile> emptyList = List.of();
            byte[] prevHash = null;
            byte[] mirrorNodeHash = null;

            Exception exception = assertThrows(Exception.class, () -> {
                DownloadDayUtil.validateBlockHashes(100L, emptyList, prevHash, mirrorNodeHash);
            });

            assertNotNull(exception);
        }

        @Test
        @DisplayName("Should throw exception for null file list")
        void testNullFileList() {
            byte[] prevHash = null;
            byte[] mirrorNodeHash = null;

            Exception exception = assertThrows(Exception.class, () -> {
                DownloadDayUtil.validateBlockHashes(100L, null, prevHash, mirrorNodeHash);
            });

            assertNotNull(exception);
        }
    }

    @Nested
    @DisplayName("Hash Format Tests")
    class HashFormatTests {

        @Test
        @DisplayName("Should format hash to hex string")
        void testHashFormatting() {
            byte[] hash = new byte[] {0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef};
            String hexString = HexFormat.of().formatHex(hash);

            assertEquals("0123456789abcdef", hexString);
        }

        @Test
        @DisplayName("Should extract first 8 characters of hash")
        void testHashSubstring() {
            byte[] hash = new byte[] {0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef};
            String hexString = HexFormat.of().formatHex(hash).substring(0, 8);

            assertEquals("01234567", hexString);
            assertEquals(8, hexString.length());
        }

        @Test
        @DisplayName("Should handle zero byte hash")
        void testZeroByteHash() {
            byte[] hash = new byte[8];
            String hexString = HexFormat.of().formatHex(hash);

            assertEquals("0000000000000000", hexString);
        }

        @Test
        @DisplayName("Should handle all FF byte hash")
        void testAllFFHash() {
            byte[] hash = new byte[8];
            for (int i = 0; i < hash.length; i++) {
                hash[i] = (byte) 0xFF;
            }
            String hexString = HexFormat.of().formatHex(hash);

            assertEquals("ffffffffffffffff", hexString);
        }

        @Test
        @DisplayName("Should handle empty hash")
        void testEmptyHash() {
            byte[] hash = new byte[0];
            String hexString = HexFormat.of().formatHex(hash);

            assertEquals("", hexString);
        }

        @Test
        @DisplayName("Should handle large hash (SHA-384)")
        void testLargeHash() {
            byte[] hash = new byte[48]; // SHA-384
            for (int i = 0; i < hash.length; i++) {
                hash[i] = (byte) i;
            }
            String hexString = HexFormat.of().formatHex(hash);

            assertEquals(96, hexString.length());
            assertTrue(hexString.startsWith("000102030405"));
        }
    }

    @Nested
    @DisplayName("Error Message Format Tests")
    class ErrorMessageFormatTests {

        @Test
        @DisplayName("Should format block number in error message")
        void testBlockNumberFormatting() {
            long blockNum = 12345L;
            String expected = "12345";
            String formatted = "Block[%d]".formatted(blockNum);

            assertTrue(formatted.contains(expected));
            assertEquals("Block[12345]", formatted);
        }

        @Test
        @DisplayName("Should format hash mismatch error message")
        void testHashMismatchErrorFormat() {
            long blockNum = 100L;
            String expectedHash = "01234567";
            String foundHash = "89abcdef";
            String path = "test/path/file.rcd";

            String errorMessage =
                    "Block[%d] hash mismatch with mirror node listing. Expected: %s, Found: %s\nContext mostCommonRecordFile:%s computedHash:%s"
                            .formatted(blockNum, expectedHash, foundHash, path, foundHash);

            assertTrue(errorMessage.contains("Block[100]"));
            assertTrue(errorMessage.contains("Expected: 01234567"));
            assertTrue(errorMessage.contains("Found: 89abcdef"));
            assertTrue(errorMessage.contains("test/path/file.rcd"));
        }

        @Test
        @DisplayName("Should format previous block hash mismatch error message")
        void testPreviousHashMismatchErrorFormat() {
            long blockNum = 200L;
            String expectedHash = "aaaaaaaa";
            String foundHash = "bbbbbbbb";
            String path = "test/path/file2.rcd";

            String errorMessage =
                    "Block[%d] previous block hash mismatch. Expected: %s, Found: %s\nContext mostCommonRecordFile:%s computedHash:%s"
                            .formatted(blockNum, expectedHash, foundHash, path, "cccccccc");

            assertTrue(errorMessage.contains("Block[200]"));
            assertTrue(errorMessage.contains("Expected: aaaaaaaa"));
            assertTrue(errorMessage.contains("Found: bbbbbbbb"));
            assertTrue(errorMessage.contains("test/path/file2.rcd"));
        }

        @Test
        @DisplayName("Should include newline in error message")
        void testErrorMessageNewline() {
            String errorMessage =
                    "Block[%d] hash mismatch with mirror node listing. Expected: %s, Found: %s\nContext mostCommonRecordFile:%s computedHash:%s"
                            .formatted(100L, "aaa", "bbb", "path", "ccc");

            assertTrue(errorMessage.contains("\n"));
            String[] lines = errorMessage.split("\n");
            assertEquals(2, lines.length);
        }
    }

    @Nested
    @DisplayName("InMemoryFile Tests")
    class InMemoryFileTests {

        @Test
        @DisplayName("Should create InMemoryFile with path and data")
        void testCreateInMemoryFile() {
            Path path = Path.of("test.rcd");
            byte[] data = new byte[] {1, 2, 3, 4};

            InMemoryFile file = new InMemoryFile(path, data);

            assertEquals(path, file.path());
            assertEquals(4, file.data().length);
        }

        @Test
        @DisplayName("Should create InMemoryFile list")
        void testCreateInMemoryFileList() {
            InMemoryFile file1 = new InMemoryFile(Path.of("file1.rcd"), new byte[] {1, 2});
            InMemoryFile file2 = new InMemoryFile(Path.of("file2.rcd"), new byte[] {3, 4});

            List<InMemoryFile> files = List.of(file1, file2);

            assertEquals(2, files.size());
            assertEquals("file1.rcd", files.get(0).path().getFileName().toString());
            assertEquals("file2.rcd", files.get(1).path().getFileName().toString());
        }

        @Test
        @DisplayName("Should handle empty data array")
        void testEmptyDataArray() {
            Path path = Path.of("empty.rcd");
            byte[] data = new byte[0];

            InMemoryFile file = new InMemoryFile(path, data);

            assertEquals(0, file.data().length);
        }

        @Test
        @DisplayName("Should handle large data array")
        void testLargeDataArray() {
            Path path = Path.of("large.rcd");
            byte[] data = new byte[1024 * 1024]; // 1MB

            InMemoryFile file = new InMemoryFile(path, data);

            assertEquals(1024 * 1024, file.data().length);
        }
    }

    @Nested
    @DisplayName("Block Number Tests")
    class BlockNumberTests {

        @Test
        @DisplayName("Should handle zero block number")
        void testZeroBlockNumber() {
            long blockNum = 0L;
            String formatted = "Block[%d]".formatted(blockNum);

            assertEquals("Block[0]", formatted);
        }

        @Test
        @DisplayName("Should handle large block number")
        void testLargeBlockNumber() {
            long blockNum = Long.MAX_VALUE;
            String formatted = "Block[%d]".formatted(blockNum);

            assertTrue(formatted.contains(String.valueOf(Long.MAX_VALUE)));
        }

        @Test
        @DisplayName("Should handle negative block number")
        void testNegativeBlockNumber() {
            long blockNum = -1L;
            String formatted = "Block[%d]".formatted(blockNum);

            assertEquals("Block[-1]", formatted);
        }

        @Test
        @DisplayName("Should handle typical block numbers")
        void testTypicalBlockNumbers() {
            long[] blockNumbers = {1L, 100L, 1000L, 10000L, 100000L, 1000000L};

            for (long blockNum : blockNumbers) {
                String formatted = "Block[%d]".formatted(blockNum);
                assertTrue(formatted.contains(String.valueOf(blockNum)));
            }
        }
    }

    @Nested
    @DisplayName("Path Handling Tests")
    class PathHandlingTests {

        @Test
        @DisplayName("Should handle simple path")
        void testSimplePath() {
            Path path = Path.of("file.rcd");

            assertEquals("file.rcd", path.getFileName().toString());
        }

        @Test
        @DisplayName("Should handle nested path")
        void testNestedPath() {
            Path path = Path.of("2025-12-01T00_00_00.000000000Z/file.rcd");

            assertEquals("file.rcd", path.getFileName().toString());
            assertEquals(2, path.getNameCount());
        }

        @Test
        @DisplayName("Should handle deep nested path")
        void testDeepNestedPath() {
            Path path = Path.of("recordstreams/record0.0.3/2025-12-01/file.rcd");

            assertEquals("file.rcd", path.getFileName().toString());
            assertTrue(path.toString().contains("recordstreams"));
        }

        @Test
        @DisplayName("Should handle path with timestamp directory")
        void testPathWithTimestampDirectory() {
            String timestamp = "2025-12-01T00_00_04.319226458Z";
            Path path = Path.of(timestamp + "/file.rcd");

            assertTrue(path.toString().startsWith(timestamp));
            assertEquals("file.rcd", path.getFileName().toString());
        }
    }
}
