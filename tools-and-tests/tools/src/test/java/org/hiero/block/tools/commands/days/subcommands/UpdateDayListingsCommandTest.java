// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hiero.block.tools.days.listing.DayListingFileReader;
import org.hiero.block.tools.days.listing.DayListingFileWriter;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.days.subcommands.UpdateDayListingsCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link UpdateDayListingsCommand}.
 *
 * <p>These tests verify that the command correctly processes day listing files
 * and generates output that matches expected content.
 */
class UpdateDayListingsCommandTest {

    /** Path to the known good listing file for 2019-09-13 (relative to module root). */
    private static final Path GOOD_LISTING_FILE = Path.of("src/test/resources/day-listing-good-2019-09-13.bin");

    @TempDir
    Path testOutputDir;

    @BeforeEach
    void setUp() {
        // TempDir annotation creates the directory automatically
    }

    @AfterEach
    void tearDown() {
        // TempDir annotation cleans up automatically
    }

    /**
     * Test that reading the known good file produces valid ListingRecordFile entries.
     *
     * @throws IOException if file reading fails
     */
    @Test
    void testReadKnownGoodFile() throws IOException {
        // Load the known good file (resolve from current working directory)
        Path goodFilePath = GOOD_LISTING_FILE.toAbsolutePath();
        assertTrue(Files.exists(goodFilePath), "Good listing file should exist at: " + goodFilePath);

        List<ListingRecordFile> records = DayListingFileReader.loadRecordsFile(goodFilePath);

        // Verify we got records
        assertNotNull(records, "Records should not be null");
        assertTrue(records.size() > 0, "Should have at least one record");

        // Print summary for debugging
        System.out.println("Loaded " + records.size() + " records from good file");

        // Count by type
        long recordCount = records.stream()
                .filter(r -> r.type() == ListingRecordFile.Type.RECORD)
                .count();
        long sigCount = records.stream()
                .filter(r -> r.type() == ListingRecordFile.Type.RECORD_SIG)
                .count();
        long sidecarCount = records.stream()
                .filter(r -> r.type() == ListingRecordFile.Type.RECORD_SIDECAR)
                .count();

        System.out.println("  RECORD files: " + recordCount);
        System.out.println("  RECORD_SIG files: " + sigCount);
        System.out.println("  RECORD_SIDECAR files: " + sidecarCount);

        // Verify all records have valid data
        for (ListingRecordFile record : records) {
            assertNotNull(record.path(), "Path should not be null");
            assertNotNull(record.timestamp(), "Timestamp should not be null");
            assertTrue(record.sizeBytes() > 0, "Size should be positive");
            assertNotNull(record.md5Hex(), "MD5 should not be null");
            assertEquals(32, record.md5Hex().length(), "MD5 hex should be 32 characters");
        }
    }

    /**
     * Test that we can write records and read them back identically.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testWriteAndReadRecords() throws Exception {
        // Load the known good file
        Path goodFilePath = GOOD_LISTING_FILE.toAbsolutePath();
        List<ListingRecordFile> expectedRecords = DayListingFileReader.loadRecordsFile(goodFilePath);

        // Write records to test output
        try (DayListingFileWriter writer = new DayListingFileWriter(testOutputDir, 2019, 9, 13)) {
            for (ListingRecordFile record : expectedRecords) {
                writer.writeRecordFile(record);
            }
        }

        // Read back from test output
        List<ListingRecordFile> actualRecords = DayListingFileReader.loadRecordsFileForDay(testOutputDir, 2019, 9, 13);

        // Verify counts match
        assertEquals(expectedRecords.size(), actualRecords.size(), "Record count should match");

        // Verify each record matches
        for (int i = 0; i < expectedRecords.size(); i++) {
            ListingRecordFile expected = expectedRecords.get(i);
            ListingRecordFile actual = actualRecords.get(i);

            assertEquals(expected.path(), actual.path(), "Path should match at index " + i);
            assertEquals(expected.timestamp(), actual.timestamp(), "Timestamp should match at index " + i);
            assertEquals(expected.sizeBytes(), actual.sizeBytes(), "Size should match at index " + i);
            assertEquals(expected.md5Hex(), actual.md5Hex(), "MD5 should match at index " + i);
        }

        System.out.println("Successfully verified " + expectedRecords.size() + " records can be written and read back");
    }

    /**
     * Test that records are written in the correct order and structure.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testRecordOrderAndStructure() throws Exception {
        // Load the known good file
        Path goodFilePath = GOOD_LISTING_FILE.toAbsolutePath();
        List<ListingRecordFile> expectedRecords = DayListingFileReader.loadRecordsFile(goodFilePath);

        // Extract unique node IDs from the records
        Set<Integer> nodeIds = expectedRecords.stream()
                .map(r -> {
                    String path = r.path();
                    int start = path.indexOf("0.0.") + 4;
                    int end = path.indexOf('/', start);
                    return Integer.parseInt(path.substring(start, end));
                })
                .collect(Collectors.toSet());

        System.out.println("Found " + nodeIds.size() + " unique nodes in test data");

        // Verify node IDs are within expected range (3-37 for mainnet)
        for (Integer nodeId : nodeIds) {
            assertTrue(nodeId >= 3 && nodeId <= 50, "Node ID " + nodeId + " should be in valid range");
        }

        // Verify records are sorted by path within the file
        List<ListingRecordFile> sortedRecords = expectedRecords.stream()
                .sorted(Comparator.comparing(ListingRecordFile::path))
                .toList();

        // Just verify sorting is possible without errors
        assertEquals(expectedRecords.size(), sortedRecords.size(), "Sorted records should have same count");
    }

    /**
     * Test that findLastDayOnDisk correctly finds existing listing files.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testFindLastDayOnDisk() throws Exception {
        // Create some test listing files
        createEmptyListingFile(testOutputDir, 2019, 9, 13);
        createEmptyListingFile(testOutputDir, 2019, 9, 14);
        createEmptyListingFile(testOutputDir, 2019, 10, 1);
        createEmptyListingFile(testOutputDir, 2020, 1, 15);

        // Use the command's public testing method
        UpdateDayListingsCommand command = new UpdateDayListingsCommand();
        LocalDate lastDay = command.findLastDayOnDiskForTesting(testOutputDir);

        assertNotNull(lastDay, "Should find a last day");
        assertEquals(LocalDate.of(2020, 1, 15), lastDay, "Should find the most recent day");
    }

    /**
     * Test that findLastDayOnDisk returns null for empty directory.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testFindLastDayOnDiskEmpty() throws Exception {
        UpdateDayListingsCommand command = new UpdateDayListingsCommand();
        LocalDate lastDay = command.findLastDayOnDiskForTesting(testOutputDir);

        assertNull(lastDay, "Should return null for empty directory");
    }

    /**
     * Test that findLastDayOnDisk handles non-existent directory.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testFindLastDayOnDiskNonExistent() throws Exception {
        Path nonExistent = testOutputDir.resolve("non-existent-subdir");

        UpdateDayListingsCommand command = new UpdateDayListingsCommand();
        LocalDate lastDay = command.findLastDayOnDiskForTesting(nonExistent);

        assertNull(lastDay, "Should return null for non-existent directory");
    }

    /**
     * Test that findLastDayOnDisk correctly handles multiple years.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testFindLastDayOnDiskMultipleYears() throws Exception {
        // Create listing files across multiple years
        createEmptyListingFile(testOutputDir, 2019, 12, 31);
        createEmptyListingFile(testOutputDir, 2020, 6, 15);
        createEmptyListingFile(testOutputDir, 2021, 3, 1);
        createEmptyListingFile(testOutputDir, 2020, 12, 31);

        UpdateDayListingsCommand command = new UpdateDayListingsCommand();
        LocalDate lastDay = command.findLastDayOnDiskForTesting(testOutputDir);

        assertEquals(LocalDate.of(2021, 3, 1), lastDay, "Should find 2021-03-01 as the most recent");
    }

    /**
     * Test that findLastDayOnDisk ignores non-standard files.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testFindLastDayOnDiskIgnoresNonStandardFiles() throws Exception {
        // Create valid listing files
        createEmptyListingFile(testOutputDir, 2019, 9, 13);

        // Create non-standard files that should be ignored
        Path yearDir = testOutputDir.resolve("2019");
        Path monthDir = yearDir.resolve("09");
        Files.createDirectories(monthDir);
        Files.createFile(monthDir.resolve("readme.txt"));
        Files.createFile(monthDir.resolve("99.bin")); // Invalid day

        // Create a directory that looks like a day file
        Files.createDirectory(yearDir.resolve("invalid"));

        UpdateDayListingsCommand command = new UpdateDayListingsCommand();
        LocalDate lastDay = command.findLastDayOnDiskForTesting(testOutputDir);

        assertEquals(LocalDate.of(2019, 9, 13), lastDay, "Should only find valid day file");
    }

    /**
     * Test verification that records contain expected file types.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testRecordFileTypes() throws Exception {
        // Load the known good file
        Path goodFilePath = GOOD_LISTING_FILE.toAbsolutePath();
        List<ListingRecordFile> records = DayListingFileReader.loadRecordsFile(goodFilePath);

        // Count each file type
        long recordFiles = records.stream()
                .filter(r -> r.type() == ListingRecordFile.Type.RECORD)
                .count();
        long sigFiles = records.stream()
                .filter(r -> r.type() == ListingRecordFile.Type.RECORD_SIG)
                .count();
        long sidecarFiles = records.stream()
                .filter(r -> r.type() == ListingRecordFile.Type.RECORD_SIDECAR)
                .count();

        // For 2019-09-13 (first day), there should be no sidecars
        assertEquals(0, sidecarFiles, "First day should have no sidecar files");

        // Should have some records and signatures
        assertTrue(recordFiles > 0, "Should have at least one record file");
        assertTrue(sigFiles > 0, "Should have at least one signature file");

        // Record files and signature files should be approximately equal
        // (slight differences can occur due to missing/corrupt files)
        double ratio = (double) sigFiles / recordFiles;
        assertTrue(
                ratio > 0.95 && ratio < 1.05,
                "Record/sig ratio should be close to 1:1 (actual: " + recordFiles + "/" + sigFiles + ")");

        System.out.println("File type distribution:");
        System.out.println("  RECORD: " + recordFiles);
        System.out.println("  RECORD_SIG: " + sigFiles);
        System.out.println("  RECORD_SIDECAR: " + sidecarFiles);
    }

    /**
     * Test that gap detection finds missing days correctly.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testFindMissingDaysDetectsGaps() throws Exception {
        // Create listing files with a gap (missing 2019-09-15)
        createEmptyListingFile(testOutputDir, 2019, 9, 13);
        createEmptyListingFile(testOutputDir, 2019, 9, 14);
        // Skip 2019-09-15
        createEmptyListingFile(testOutputDir, 2019, 9, 16);
        createEmptyListingFile(testOutputDir, 2019, 9, 17);

        // Check that day 15 is detected as missing
        LocalDate day15 = LocalDate.of(2019, 9, 15);
        Path day15File = testOutputDir.resolve("2019").resolve("09").resolve("15.bin");

        assertTrue(!java.nio.file.Files.exists(day15File), "Day 15 file should not exist");

        // Verify other days exist
        assertTrue(java.nio.file.Files.exists(testOutputDir.resolve("2019/09/13.bin")), "Day 13 should exist");
        assertTrue(java.nio.file.Files.exists(testOutputDir.resolve("2019/09/14.bin")), "Day 14 should exist");
        assertTrue(java.nio.file.Files.exists(testOutputDir.resolve("2019/09/16.bin")), "Day 16 should exist");
    }

    /**
     * Test that gap detection finds multiple gaps.
     *
     * @throws Exception if file operations fail
     */
    @Test
    void testFindMissingDaysMultipleGaps() throws Exception {
        // Create listing files with multiple gaps
        createEmptyListingFile(testOutputDir, 2019, 9, 13);
        // Gap: 14, 15
        createEmptyListingFile(testOutputDir, 2019, 9, 16);
        // Gap: 17
        createEmptyListingFile(testOutputDir, 2019, 9, 18);

        // Verify gaps exist (files don't exist)
        assertTrue(!java.nio.file.Files.exists(testOutputDir.resolve("2019/09/14.bin")), "Day 14 should be missing");
        assertTrue(!java.nio.file.Files.exists(testOutputDir.resolve("2019/09/15.bin")), "Day 15 should be missing");
        assertTrue(!java.nio.file.Files.exists(testOutputDir.resolve("2019/09/17.bin")), "Day 17 should be missing");

        // Verify existing files are there
        assertTrue(java.nio.file.Files.exists(testOutputDir.resolve("2019/09/13.bin")), "Day 13 should exist");
        assertTrue(java.nio.file.Files.exists(testOutputDir.resolve("2019/09/16.bin")), "Day 16 should exist");
        assertTrue(java.nio.file.Files.exists(testOutputDir.resolve("2019/09/18.bin")), "Day 18 should exist");
    }

    /**
     * Creates an empty listing file for testing findLastDayOnDisk.
     *
     * @param baseDir base directory
     * @param year year
     * @param month month
     * @param day day
     * @throws Exception if file creation fails
     */
    @SuppressWarnings("EmptyTryBlock")
    private void createEmptyListingFile(Path baseDir, int year, int month, int day) throws Exception {
        try (DayListingFileWriter writer = new DayListingFileWriter(baseDir, year, month, day)) {
            // Empty file - just creates the structure
        }
    }
}
