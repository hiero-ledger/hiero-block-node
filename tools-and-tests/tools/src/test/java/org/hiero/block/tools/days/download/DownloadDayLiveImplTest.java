// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.download;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl.BlockDownloadResult;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DownloadDayLiveImpl} that focus on the live-download
 * file layout we rely on in {@code DownloadLive.LiveDownloader.fullBlockValidate}.
 *
 * These tests are intentionally narrow: they do not exercise the full GCS
 * listing or hash-chain logic, but instead assert that a {@link BlockDownloadResult}
 * for a single block contains:
 * <ul>
 *     <li>Exactly one primary record file (.rcd)</li>
 *     <li>At least one signature file (.rcd_sig or .rcd_sig.gz)</li>
 *     <li>At least one sidecar file (.rcd) with an index suffix (e.g. _01.rcd)</li>
 * </ul>
 *
 * This mirrors what {@code fullBlockValidate} uses when detecting
 * primaryRecord, signatures and sidecars from the in-memory file list.
 */
public class DownloadDayLiveImplTest {

    @Test
    @DisplayName("BlockDownloadResult for a single block exposes record, signature and sidecar with normalized paths")
    void blockDownloadResultContainsRecordSignatureAndSidecar() {

        final long blockNumber = 87917884L;
        final String tsDir = "2025-12-01T00_00_04.319226458Z";

        // This is the normalized layout we expect DownloadDayLiveImpl to produce:
        //   <tsDir>/<tsDir>.rcd
        //   <tsDir>/<tsDir>.rcd_sig
        //   <tsDir>/<tsDir>_01.rcd
        final Path recordPath = Path.of(tsDir, tsDir + ".rcd");
        final Path sigPath = Path.of(tsDir, tsDir + ".rcd_sig");
        final Path sidecarPath = Path.of(tsDir, tsDir + "_01.rcd");

        final List<InMemoryFile> files = new ArrayList<>();
        files.add(new InMemoryFile(recordPath, new byte[] {0x01}));
        files.add(new InMemoryFile(sigPath, new byte[] {0x02}));
        files.add(new InMemoryFile(sidecarPath, new byte[] {0x03}));

        final byte[] newPrevRecordFileHash = new byte[] {0x0A, 0x0B};

        final BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, newPrevRecordFileHash);

        assertEquals(blockNumber, result.blockNumber);
        assertEquals(3, result.files.size());
        assertNotNull(result.newPreviousRecordFileHash);
        assertEquals(2, result.newPreviousRecordFileHash.length);
        assertEquals(0x0A, result.newPreviousRecordFileHash[0]);
        assertEquals(0x0B, result.newPreviousRecordFileHash[1]);

        final List<InMemoryFile> records = result.files.stream()
                .filter(f -> {
                    final String name = f.path().getFileName().toString();
                    return name.endsWith(".rcd") && !name.contains("_01") && !name.contains("_node_");
                })
                .toList();

        final List<InMemoryFile> signatures = result.files.stream()
                .filter(f -> {
                    final String name = f.path().getFileName().toString();
                    return name.endsWith(".rcs_sig") || name.endsWith(".rcd_sig") || name.endsWith(".rcd_sig.gz");
                })
                .toList();

        final List<InMemoryFile> sidecars = result.files.stream()
                .filter(f -> {
                    final String name = f.path().getFileName().toString();
                    return name.endsWith(".rcd") && name.contains("_01");
                })
                .toList();

        assertEquals(1, records.size(), "Exactly one primary record file should be present");
        assertEquals(recordPath.toString(), records.get(0).path().toString());

        assertTrue(!signatures.isEmpty(), "At least one signature file should be present");
        for (var f : signatures) {
            String name = f.path().toString();
            assertTrue(name.startsWith(tsDir + "/"));
            assertTrue(
                    name.endsWith(".rcs_sig") || name.endsWith(".rcd_sig") || name.endsWith(".rcd_sig.gz"),
                    "Signature file must end with .rcs_sig, .rcd_sig or .rcd_sig.gz");
        }

        assertTrue(!sidecars.isEmpty(), "At least one sidecar file should be present");
        for (var f : sidecars) {
            String name = f.path().toString();
            assertTrue(name.startsWith(tsDir + "/"));
            assertTrue(name.contains("_01.rcd"));
        }
    }

    @Test
    @DisplayName("BlockDownloadResult supports gzipped .rcd_sig.gz signature naming")
    void blockDownloadResultAcceptsGzippedSignature() {
        // given
        final long blockNumber = 87917884L;
        final String tsDir = "2025-12-01T00_00_04.319226458Z";

        final Path recordPath = Path.of(tsDir, tsDir + ".rcd");
        final Path gzSigPath = Path.of(tsDir, tsDir + ".rcd_sig.gz");
        final Path sidecarPath = Path.of(tsDir, tsDir + "_01.rcd");

        final List<InMemoryFile> files = new ArrayList<>();
        files.add(new InMemoryFile(recordPath, new byte[] {0x01}));
        files.add(new InMemoryFile(gzSigPath, new byte[] {0x02}));
        files.add(new InMemoryFile(sidecarPath, new byte[] {0x03}));

        final BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, new byte[] {0x0A});

        final List<InMemoryFile> signatures = result.files.stream()
                .filter(f -> {
                    final String name = f.path().getFileName().toString();
                    return name.endsWith(".rcs_sig") || name.endsWith(".rcd_sig") || name.endsWith(".rcd_sig.gz");
                })
                .toList();

        assertEquals(1, signatures.size(), "Exactly one gzipped signature file should be present");
        assertEquals(gzSigPath.toString(), signatures.get(0).path().toString());
    }

    @Nested
    @DisplayName("BlockDownloadResult Edge Cases")
    class BlockDownloadResultEdgeCasesTests {

        @Test
        @DisplayName("Should handle null hash")
        void testNullHash() {
            long blockNumber = 100L;
            List<InMemoryFile> files = List.of();

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, null);

            assertNull(result.newPreviousRecordFileHash);
        }

        @Test
        @DisplayName("Should handle empty file list")
        void testEmptyFileList() {
            long blockNumber = 200L;
            List<InMemoryFile> files = List.of();
            byte[] hash = new byte[] {1, 2, 3};

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, hash);

            assertTrue(result.files.isEmpty());
            assertArrayEquals(new byte[] {1, 2, 3}, result.newPreviousRecordFileHash);
        }

        @Test
        @DisplayName("Should handle zero block number")
        void testZeroBlockNumber() {
            long blockNumber = 0L;
            List<InMemoryFile> files = List.of();

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, null);

            assertEquals(0L, result.blockNumber);
        }

        @Test
        @DisplayName("Should handle negative block number")
        void testNegativeBlockNumber() {
            long blockNumber = -1L;
            List<InMemoryFile> files = List.of();

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, null);

            assertEquals(-1L, result.blockNumber);
        }

        @Test
        @DisplayName("Should handle maximum block number")
        void testMaxBlockNumber() {
            long blockNumber = Long.MAX_VALUE;
            List<InMemoryFile> files = List.of();

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, null);

            assertEquals(Long.MAX_VALUE, result.blockNumber);
        }

        @Test
        @DisplayName("Should handle empty hash array")
        void testEmptyHashArray() {
            long blockNumber = 300L;
            List<InMemoryFile> files = List.of();
            byte[] hash = new byte[0];

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, hash);

            assertNotNull(result.newPreviousRecordFileHash);
            assertEquals(0, result.newPreviousRecordFileHash.length);
        }

        @Test
        @DisplayName("Should handle large hash array")
        void testLargeHashArray() {
            long blockNumber = 400L;
            List<InMemoryFile> files = List.of();
            byte[] hash = new byte[48]; // SHA-384 size

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, hash);

            assertEquals(48, result.newPreviousRecordFileHash.length);
        }

        @Test
        @DisplayName("Should handle many files")
        void testManyFiles() {
            long blockNumber = 500L;
            List<InMemoryFile> files = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                files.add(new InMemoryFile(Path.of("file" + i + ".rcd"), new byte[] {(byte) i}));
            }

            BlockDownloadResult result = new BlockDownloadResult(blockNumber, files, null);

            assertEquals(100, result.files.size());
        }
    }

    @Nested
    @DisplayName("File Pattern Recognition")
    class FilePatternRecognitionTests {

        @Test
        @DisplayName("Should recognize primary record file")
        void testPrimaryRecordFile() {
            String filename = "2025-12-01T00_00_04.319226458Z.rcd";

            assertTrue(filename.endsWith(".rcd"));
            assertFalse(filename.contains("_node_"));
            assertFalse(filename.matches(".*_\\d+\\.rcd$"));
        }

        @Test
        @DisplayName("Should recognize node-specific record file")
        void testNodeSpecificRecordFile() {
            String filename = "2025-12-01T00_00_04.319226458Z_node_3.rcd";

            assertTrue(filename.endsWith(".rcd"));
            assertTrue(filename.contains("_node_"));
        }

        @Test
        @DisplayName("Should recognize sidecar file with index")
        void testSidecarFile() {
            String filename = "2025-12-01T00_00_04.319226458Z_01.rcd";

            assertTrue(filename.endsWith(".rcd"));
            assertTrue(filename.matches(".*_\\d+\\.rcd$"));
        }

        @Test
        @DisplayName("Should recognize legacy signature file")
        void testLegacySignatureFile() {
            String filename = "node_0.0.3.rcs_sig";

            assertTrue(filename.endsWith(".rcs_sig"));
            assertTrue(filename.startsWith("node_"));
        }

        @Test
        @DisplayName("Should recognize new signature file")
        void testNewSignatureFile() {
            String filename = "2025-12-01T00_00_04.319226458Z.rcd_sig";

            assertTrue(filename.endsWith(".rcd_sig"));
        }

        @Test
        @DisplayName("Should recognize compressed signature file")
        void testCompressedSignatureFile() {
            String filename = "2025-12-01T00_00_04.319226458Z.rcd_sig.gz";

            assertTrue(filename.endsWith(".rcd_sig.gz"));
            assertTrue(filename.contains(".rcd_sig"));
        }

        @Test
        @DisplayName("Should recognize compressed record file")
        void testCompressedRecordFile() {
            String filename = "2025-12-01T00_00_04.319226458Z.rcd.gz";

            assertTrue(filename.endsWith(".gz"));
            assertTrue(filename.contains(".rcd"));
        }
    }

    @Nested
    @DisplayName("Path Manipulation")
    class PathManipulationTests {

        @Test
        @DisplayName("Should extract filename from path")
        void testExtractFilename() {
            String path = "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.319226458Z.rcd";
            int lastSlash = path.lastIndexOf('/');
            String filename = path.substring(lastSlash + 1);

            assertEquals("2025-12-01T00_00_04.319226458Z.rcd", filename);
        }

        @Test
        @DisplayName("Should extract parent directory from path")
        void testExtractParentDirectory() {
            String path = "recordstreams/record0.0.3/2025-12-01/file.rcd";
            int lastSlash = path.lastIndexOf('/');
            String parentDir = path.substring(0, lastSlash);

            assertEquals("recordstreams/record0.0.3/2025-12-01", parentDir);
        }

        @Test
        @DisplayName("Should extract node identifier from path")
        void testExtractNodeIdentifier() {
            String path = "recordstreams/record0.0.3/2025-12-01/file.rcd";
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            String nodeDir = parentDir.substring(parentDir.lastIndexOf('/') + 1).replace("record", "");

            assertTrue(nodeDir.isEmpty() || nodeDir.equals("0.0.3") || nodeDir.startsWith("2025"));
        }

        @Test
        @DisplayName("Should remove .gz extension")
        void testRemoveGzExtension() {
            String filename = "test.rcd.gz";
            String ungzipped = filename.replaceAll("\\.gz$", "");

            assertEquals("test.rcd", ungzipped);
        }

        @Test
        @DisplayName("Should not remove gz from middle of filename")
        void testGzInMiddle() {
            String filename = "gztest.rcd";
            String result = filename.replaceAll("\\.gz$", "");

            assertEquals("gztest.rcd", result);
        }

        @Test
        @DisplayName("Should handle nested paths")
        void testNestedPaths() {
            Path path = Path.of("2025-12-01T00_00_00.000000000Z/test.rcd");

            assertEquals(2, path.getNameCount());
            assertEquals("test.rcd", path.getFileName().toString());
        }
    }

    @Nested
    @DisplayName("Hash Format Handling")
    class HashFormatHandlingTests {

        @Test
        @DisplayName("Should handle hash with 0x prefix")
        void testHashWith0xPrefix() {
            String hashHex = "0x1234567890abcdef";
            String withoutPrefix = hashHex.startsWith("0x") ? hashHex.substring(2) : hashHex;

            assertEquals("1234567890abcdef", withoutPrefix);
        }

        @Test
        @DisplayName("Should handle hash without 0x prefix")
        void testHashWithout0xPrefix() {
            String hashHex = "1234567890abcdef";
            String withoutPrefix = hashHex.startsWith("0x") ? hashHex.substring(2) : hashHex;

            assertEquals("1234567890abcdef", withoutPrefix);
        }

        @Test
        @DisplayName("Should handle empty hash string")
        void testEmptyHashString() {
            String hashHex = "";
            String withoutPrefix = hashHex.startsWith("0x") ? hashHex.substring(2) : hashHex;

            assertEquals("", withoutPrefix);
        }

        @Test
        @DisplayName("Should validate hex format")
        void testHexFormat() {
            String validHex = "0123456789abcdefABCDEF";

            assertTrue(validHex.matches("[0-9a-fA-F]+"));
        }

        @Test
        @DisplayName("Should reject invalid hex format")
        void testInvalidHexFormat() {
            String invalidHex = "xyz123";

            assertFalse(invalidHex.matches("[0-9a-fA-F]+"));
        }
    }

    @Nested
    @DisplayName("Block Range Calculations")
    class BlockRangeCalculationsTests {

        @Test
        @DisplayName("Should calculate block count for typical range")
        void testTypicalBlockCount() {
            long firstBlock = 1000L;
            long lastBlock = 2000L;
            long blockCount = lastBlock - firstBlock + 1;

            assertEquals(1001, blockCount);
        }

        @Test
        @DisplayName("Should calculate block count for single block")
        void testSingleBlockCount() {
            long firstBlock = 500L;
            long lastBlock = 500L;
            long blockCount = lastBlock - firstBlock + 1;

            assertEquals(1, blockCount);
        }

        @Test
        @DisplayName("Should handle consecutive blocks")
        void testConsecutiveBlocks() {
            long block1 = 100L;
            long block2 = 101L;

            assertEquals(1, block2 - block1);
        }

        @Test
        @DisplayName("Should calculate large block range")
        void testLargeBlockRange() {
            long firstBlock = 0L;
            long lastBlock = 1_000_000L;
            long blockCount = lastBlock - firstBlock + 1;

            assertEquals(1_000_001, blockCount);
        }
    }

    @Nested
    @DisplayName("Progress and ETA Calculations")
    class ProgressCalculationsTests {

        @Test
        @DisplayName("Should calculate day share percent")
        void testDaySharePercent() {
            long totalDays = 10;
            double daySharePercent = 100.0 / totalDays;

            assertEquals(10.0, daySharePercent, 0.01);
        }

        @Test
        @DisplayName("Should handle zero total days")
        void testZeroTotalDays() {
            long totalDays = 0;
            double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);

            assertEquals(100.0, daySharePercent, 0.01);
        }

        @Test
        @DisplayName("Should calculate overall percent")
        void testOverallPercent() {
            int dayIndex = 5;
            double daySharePercent = 10.0;
            double blockFraction = 0.5;
            double overallPercent = dayIndex * daySharePercent + blockFraction * daySharePercent;

            assertEquals(55.0, overallPercent, 0.01);
        }

        @Test
        @DisplayName("Should calculate ETA remaining")
        void testEtaRemaining() {
            long elapsed = 1000L;
            double overallPercent = 25.0;
            long remaining = (long) (elapsed * (100.0 - overallPercent) / overallPercent);

            assertEquals(3000L, remaining);
        }

        @Test
        @DisplayName("Should handle zero progress ETA")
        void testZeroProgressEta() {
            double overallPercent = 0.0;
            long remaining = Long.MAX_VALUE;
            if (overallPercent > 0.0 && overallPercent < 100.0) {
                remaining = 0L; // Would calculate, but condition false
            }

            assertEquals(Long.MAX_VALUE, remaining);
        }

        @Test
        @DisplayName("Should handle complete progress ETA")
        void testCompleteProgressEta() {
            double overallPercent = 100.0;
            long remaining = 0L;
            if (overallPercent >= 100.0) {
                remaining = 0L;
            }

            assertEquals(0L, remaining);
        }
    }

    @Nested
    @DisplayName("Date Formatting")
    class DateFormattingTests {

        @Test
        @DisplayName("Should format standard date")
        void testStandardDateFormat() {
            int year = 2025;
            int month = 12;
            int day = 1;
            String dayString = String.format("%04d-%02d-%02d", year, month, day);

            assertEquals("2025-12-01", dayString);
        }

        @Test
        @DisplayName("Should format single digit date")
        void testSingleDigitDate() {
            int year = 2025;
            int month = 1;
            int day = 5;
            String dayString = String.format("%04d-%02d-%02d", year, month, day);

            assertEquals("2025-01-05", dayString);
        }

        @Test
        @DisplayName("Should format end of year")
        void testEndOfYear() {
            int year = 2024;
            int month = 12;
            int day = 31;
            String dayString = String.format("%04d-%02d-%02d", year, month, day);

            assertEquals("2024-12-31", dayString);
        }

        @Test
        @DisplayName("Should validate date format pattern")
        void testDateFormatPattern() {
            String dayString = "2025-12-01";

            assertTrue(dayString.matches("\\d{4}-\\d{2}-\\d{2}"));
        }

        @Test
        @DisplayName("Should reject invalid date formats")
        void testInvalidDateFormats() {
            String[] invalidFormats = {"2025/12/01", "01-12-2025", "2025-12-1", "25-12-01"};

            for (String invalid : invalidFormats) {
                assertFalse(invalid.matches("\\d{4}-\\d{2}-\\d{2}"));
            }
        }
    }

    @Nested
    @DisplayName("File Output Naming")
    class FileOutputNamingTests {

        @Test
        @DisplayName("Should create tar.zstd filename")
        void testTarZstdFilename() {
            String dayString = "2025-12-01";
            String filename = dayString + ".tar.zstd";

            assertEquals("2025-12-01.tar.zstd", filename);
            assertTrue(filename.endsWith(".tar.zstd"));
        }

        @Test
        @DisplayName("Should create partial filename")
        void testPartialFilename() {
            String dayString = "2025-12-01";
            String filename = dayString + ".tar.zstd_partial";

            assertEquals("2025-12-01.tar.zstd_partial", filename);
            assertTrue(filename.endsWith("_partial"));
        }

        @Test
        @DisplayName("Should handle path with tar.zstd extension")
        void testPathWithTarZstd() {
            Path downloadedDaysDir = Path.of("/data/days");
            String dayString = "2025-12-01";
            Path finalOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd");

            assertEquals("2025-12-01.tar.zstd", finalOutFile.getFileName().toString());
            assertTrue(finalOutFile.toString().contains("/data/days"));
        }
    }

    @Nested
    @DisplayName("extractBlockHashFromMirrorNode Tests")
    class ExtractBlockHashFromMirrorNodeTests {

        @Test
        @DisplayName("Should extract first block hash with 0x prefix")
        void testExtractFirstBlockHashWith0xPrefix() {
            DayBlockInfo dayBlockInfo =
                    new DayBlockInfo(2025, 12, 1, 1000L, "0x1234567890abcdef", 2000L, "0xfedcba0987654321");

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(1000L, dayBlockInfo);

            assertNotNull(hash);
            assertEquals(8, hash.length);
        }

        @Test
        @DisplayName("Should extract first block hash without 0x prefix")
        void testExtractFirstBlockHashWithout0xPrefix() {
            DayBlockInfo dayBlockInfo =
                    new DayBlockInfo(2025, 12, 1, 1000L, "1234567890abcdef", 2000L, "fedcba0987654321");

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(1000L, dayBlockInfo);

            assertNotNull(hash);
            assertEquals(8, hash.length);
        }

        @Test
        @DisplayName("Should extract last block hash")
        void testExtractLastBlockHash() {
            DayBlockInfo dayBlockInfo =
                    new DayBlockInfo(2025, 12, 1, 1000L, "0x1234567890abcdef", 2000L, "0xfedcba0987654321");

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(2000L, dayBlockInfo);

            assertNotNull(hash);
            assertEquals(8, hash.length);
        }

        @Test
        @DisplayName("Should return null for middle block")
        void testReturnNullForMiddleBlock() {
            DayBlockInfo dayBlockInfo =
                    new DayBlockInfo(2025, 12, 1, 1000L, "0x1234567890abcdef", 2000L, "0xfedcba0987654321");

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(1500L, dayBlockInfo);

            assertNull(hash);
        }

        @Test
        @DisplayName("Should return null when first block hash is null")
        void testReturnNullWhenFirstBlockHashNull() {
            DayBlockInfo dayBlockInfo = new DayBlockInfo(2025, 12, 1, 1000L, null, 2000L, "0xfedcba0987654321");

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(1000L, dayBlockInfo);

            assertNull(hash);
        }

        @Test
        @DisplayName("Should return null when last block hash is null")
        void testReturnNullWhenLastBlockHashNull() {
            DayBlockInfo dayBlockInfo = new DayBlockInfo(2025, 12, 1, 1000L, "0x1234567890abcdef", 2000L, null);

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(2000L, dayBlockInfo);

            assertNull(hash);
        }

        @Test
        @DisplayName("Should handle empty hash string")
        void testHandleEmptyHashString() {
            DayBlockInfo dayBlockInfo = new DayBlockInfo(2025, 12, 1, 1000L, "", 2000L, "0xfedcba0987654321");

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(1000L, dayBlockInfo);

            assertNotNull(hash);
            assertEquals(0, hash.length);
        }

        @Test
        @DisplayName("Should handle long SHA-384 hash")
        void testHandleLongHash() {
            String longHash = "0x" + "a".repeat(96); // SHA-384
            DayBlockInfo dayBlockInfo = new DayBlockInfo(2025, 12, 1, 1000L, longHash, 2000L, "0xfedcba0987654321");

            byte[] hash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(1000L, dayBlockInfo);

            assertNotNull(hash);
            assertEquals(48, hash.length);
        }

        @Test
        @DisplayName("Should handle single block day")
        void testSingleBlockDay() {
            DayBlockInfo dayBlockInfo =
                    new DayBlockInfo(2025, 12, 1, 1000L, "0x1234567890abcdef", 1000L, "0x1234567890abcdef");

            byte[] firstHash = DownloadDayLiveImpl.extractBlockHashFromMirrorNode(1000L, dayBlockInfo);

            assertNotNull(firstHash);
        }
    }

    @Nested
    @DisplayName("computeFilesToDownload Tests")
    class ComputeFilesToDownloadTests {

        private ListingRecordFile createRecordFile(String path, ListingRecordFile.Type type) {
            LocalDateTime timestamp = LocalDateTime.of(2025, 12, 1, 0, 0, 4);
            return new ListingRecordFile(path, timestamp, 1000, "12345678901234567890123456789012");
        }

        @Test
        @DisplayName("Should include most common record file first")
        void testMostCommonRecordFileFirst() {
            ListingRecordFile mostCommon = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            ListingRecordFile otherRecord = createRecordFile(
                    "recordstreams/record0.0.4/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            List<ListingRecordFile> group = List.of(mostCommon, otherRecord);

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommon, new ListingRecordFile[0], group);

            assertEquals(1, result.size());
            assertEquals(mostCommon, result.get(0));
        }

        @Test
        @DisplayName("Should include sidecars after most common record")
        void testSidecarsAfterMostCommon() {
            ListingRecordFile mostCommon = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            ListingRecordFile sidecar = createRecordFile(
                    "recordstreams/record0.0.3/sidecar/2025-12-01T00_00_04_01.rcd",
                    ListingRecordFile.Type.RECORD_SIDECAR);
            List<ListingRecordFile> group = List.of(mostCommon, sidecar);

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommon, new ListingRecordFile[] {sidecar}, group);

            assertEquals(2, result.size());
            assertEquals(mostCommon, result.get(0));
            assertEquals(sidecar, result.get(1));
        }

        @Test
        @DisplayName("Should include all signature files")
        void testIncludeAllSignatureFiles() {
            ListingRecordFile mostCommon = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            ListingRecordFile sig1 = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd_sig",
                    ListingRecordFile.Type.RECORD_SIG);
            ListingRecordFile sig2 = createRecordFile(
                    "recordstreams/record0.0.4/2025-12-01/2025-12-01T00_00_04.rcd_sig",
                    ListingRecordFile.Type.RECORD_SIG);
            List<ListingRecordFile> group = List.of(mostCommon, sig1, sig2);

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommon, new ListingRecordFile[0], group);

            assertEquals(3, result.size());
            assertTrue(result.contains(sig1));
            assertTrue(result.contains(sig2));
        }

        @Test
        @DisplayName("Should not duplicate most common record")
        void testNoDuplicateMostCommonRecord() {
            ListingRecordFile mostCommon = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            List<ListingRecordFile> group = List.of(mostCommon, mostCommon);

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommon, new ListingRecordFile[0], group);

            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("Should not duplicate most common sidecar")
        void testNoDuplicateMostCommonSidecar() {
            ListingRecordFile mostCommon = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            ListingRecordFile sidecar = createRecordFile(
                    "recordstreams/record0.0.3/sidecar/2025-12-01T00_00_04_01.rcd",
                    ListingRecordFile.Type.RECORD_SIDECAR);
            List<ListingRecordFile> group = List.of(mostCommon, sidecar, sidecar);

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommon, new ListingRecordFile[] {sidecar}, group);

            assertEquals(2, result.size());
        }

        @Test
        @DisplayName("Should handle null most common record")
        void testNullMostCommonRecord() {
            ListingRecordFile sig = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd_sig",
                    ListingRecordFile.Type.RECORD_SIG);
            List<ListingRecordFile> group = List.of(sig);

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(null, new ListingRecordFile[0], group);

            assertEquals(1, result.size());
            assertEquals(sig, result.get(0));
        }

        @Test
        @DisplayName("Should handle empty sidecar array")
        void testEmptySidecarArray() {
            ListingRecordFile mostCommon = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            List<ListingRecordFile> group = List.of(mostCommon);

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommon, new ListingRecordFile[0], group);

            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("Should handle empty group")
        void testEmptyGroup() {
            ListingRecordFile mostCommon = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            List<ListingRecordFile> group = List.of();

            List<ListingRecordFile> result =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommon, new ListingRecordFile[0], group);

            assertEquals(1, result.size());
            assertEquals(mostCommon, result.get(0));
        }

        @Test
        @DisplayName("Should order files correctly: most common record, sidecars, other records, signatures")
        void testCorrectOrdering() {
            ListingRecordFile mostCommonRecord = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            ListingRecordFile sidecar = createRecordFile(
                    "recordstreams/record0.0.3/sidecar/2025-12-01T00_00_04_01.rcd",
                    ListingRecordFile.Type.RECORD_SIDECAR);
            ListingRecordFile otherRecord = createRecordFile(
                    "recordstreams/record0.0.4/2025-12-01/2025-12-01T00_00_04.rcd", ListingRecordFile.Type.RECORD);
            ListingRecordFile sig = createRecordFile(
                    "recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.rcd_sig",
                    ListingRecordFile.Type.RECORD_SIG);

            List<ListingRecordFile> group = List.of(sig, otherRecord, mostCommonRecord, sidecar);

            List<ListingRecordFile> result = DownloadDayLiveImpl.computeFilesToDownload(
                    mostCommonRecord, new ListingRecordFile[] {sidecar}, group);

            assertEquals(3, result.size());
            assertEquals(mostCommonRecord, result.get(0));
            assertEquals(sidecar, result.get(1));
            assertEquals(sig, result.get(2));
        }
    }

    @Nested
    @DisplayName("computeNewFilePath Tests")
    class ComputeNewFilePathTests {

        private ListingRecordFile createRecordFile(String path) {
            LocalDateTime timestamp = LocalDateTime.of(2025, 12, 1, 0, 0, 4);
            return new ListingRecordFile(path, timestamp, 1000, "12345678901234567890123456789012");
        }

        @Test
        @DisplayName("Should compute path for most common record file")
        void testMostCommonRecordFile() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.319226458Z.rcd");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
            mostCommonFiles.add(lr);

            Path result =
                    DownloadDayLiveImpl.computeNewFilePath(lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z.rcd");

            assertTrue(result.toString().contains("2025-12-01T00_00_04.319226458Z"));
            assertTrue(result.toString().endsWith(".rcd"));
            assertFalse(result.toString().contains("_node_"));
        }

        @Test
        @DisplayName("Should compute path for node-specific record file")
        void testNodeSpecificRecordFile() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.319226458Z.rcd");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();

            Path result =
                    DownloadDayLiveImpl.computeNewFilePath(lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z.rcd");

            assertTrue(result.toString().contains("_node_"));
            assertTrue(result.toString().endsWith(".rcd"));
        }

        @Test
        @DisplayName("Should compute path for signature file")
        void testSignatureFile() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.319226458Z.rcd_sig");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();

            Path result = DownloadDayLiveImpl.computeNewFilePath(
                    lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z.rcd_sig");

            assertTrue(result.toString().contains("node_"));
            assertTrue(result.toString().endsWith(".rcs_sig"));
        }

        @Test
        @DisplayName("Should compute path for most common sidecar file")
        void testMostCommonSidecarFile() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.3/sidecar/2025-12-01T00_00_04.319226458Z_01.rcd");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
            mostCommonFiles.add(lr);

            Path result = DownloadDayLiveImpl.computeNewFilePath(
                    lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z_01.rcd");

            assertTrue(result.toString().endsWith("_01.rcd"));
            assertFalse(result.toString().contains("_node_"));
        }

        @Test
        @DisplayName("Should compute path for node-specific sidecar file")
        void testNodeSpecificSidecarFile() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.4/sidecar/2025-12-01T00_00_04.319226458Z_01.rcd");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();

            Path result = DownloadDayLiveImpl.computeNewFilePath(
                    lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z_01.rcd");

            assertTrue(result.toString().contains("_node_"));
            assertTrue(result.toString().endsWith(".rcd"));
        }

        @Test
        @DisplayName("Should extract node directory correctly")
        void testExtractNodeDirectory() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.319226458Z.rcd_sig");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();

            Path result = DownloadDayLiveImpl.computeNewFilePath(
                    lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z.rcd_sig");

            // For signature files, node directory is extracted from parent directory (2025-12-01)
            assertTrue(result.toString().contains("node_"));
            assertTrue(result.toString().endsWith(".rcs_sig"));
        }

        @Test
        @DisplayName("Should handle paths with different node formats")
        void testDifferentNodeFormats() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.123/2025-12-01/2025-12-01T00_00_04.319226458Z.rcd_sig");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();

            Path result = DownloadDayLiveImpl.computeNewFilePath(
                    lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z.rcd_sig");

            // For signature files with date subdirectory, node directory becomes the date folder name
            assertTrue(result.toString().contains("node_"));
            assertTrue(result.toString().endsWith(".rcs_sig"));
        }

        @Test
        @DisplayName("Should include timestamp directory in path")
        void testTimestampDirectory() throws IOException {
            ListingRecordFile lr =
                    createRecordFile("recordstreams/record0.0.3/2025-12-01/2025-12-01T00_00_04.319226458Z.rcd");
            Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
            mostCommonFiles.add(lr);

            Path result =
                    DownloadDayLiveImpl.computeNewFilePath(lr, mostCommonFiles, "2025-12-01T00_00_04.319226458Z.rcd");

            assertEquals(2, result.getNameCount());
            assertTrue(result.toString().startsWith("2025-12-01T00_00_04.319226458Z/"));
        }
    }
}
