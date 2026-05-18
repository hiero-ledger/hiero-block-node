// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.listing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DayListingFileWriter}.
 *
 * <p>Verifies that listing files are correctly truncated when regenerated
 * to prevent corruption from appending to existing files.
 */
class DayListingFileWriterTest {

    @TempDir
    Path tempDir;

    /**
     * Tests that writing to the same listing file twice truncates the file
     * instead of appending, preventing corruption.
     */
    @Test
    void testTruncateExistingPreventsCorruption() throws Exception {
        int year = 2024;
        int month = 1;
        int day = 15;

        // First write: Create a file with 2 entries
        try (DayListingFileWriter writer = new DayListingFileWriter(tempDir, year, month, day)) {
            writer.writeRecordFile(createTestRecordFile("record0.0.3/2024-01-15T10_00_00.123456789Z.rcd.gz", 2024, 1, 15, 10, 0, 0));
            writer.writeRecordFile(createTestRecordFile("record0.0.3/2024-01-15T10_00_01.987654321Z.rcd.gz", 2024, 1, 15, 10, 0, 1));
        }

        Path listingFile = ListingRecordFile.getFileForDay(tempDir, year, month, day);
        assertTrue(Files.exists(listingFile), "Listing file should exist");
        long firstSize = Files.size(listingFile);

        // Read first write to verify 2 entries
        try (DataInputStream in = new DataInputStream(Files.newInputStream(listingFile))) {
            long count = in.readLong();
            assertEquals(2, count, "First write should have 2 entries");
        }

        // Second write: Write 1 entry to the same file
        // Without TRUNCATE_EXISTING, this would append and corrupt the file
        try (DayListingFileWriter writer = new DayListingFileWriter(tempDir, year, month, day)) {
            writer.writeRecordFile(createTestRecordFile("record0.0.3/2024-01-15T10_00_02.111111111Z.rcd.gz", 2024, 1, 15, 10, 0, 2));
        }

        long secondSize = Files.size(listingFile);
        assertTrue(secondSize < firstSize, "File should be smaller after truncation (1 entry vs 2)");

        // Verify the file was truncated and only has the new entry
        try (DataInputStream in = new DataInputStream(Files.newInputStream(listingFile))) {
            long count = in.readLong();
            assertEquals(1, count, "Second write should have truncated and written 1 entry, not appended");
        }
    }

    /**
     * Tests basic write and read functionality.
     */
    @Test
    void testBasicWriteAndRead() throws Exception {
        int year = 2024;
        int month = 3;
        int day = 10;

        // Write 3 entries
        try (DayListingFileWriter writer = new DayListingFileWriter(tempDir, year, month, day)) {
            writer.writeRecordFile(createTestRecordFile("record0.0.3/2024-03-10T08_00_00.000000000Z.rcd.gz", 2024, 3, 10, 8, 0, 0));
            writer.writeRecordFile(createTestRecordFile("record0.0.3/2024-03-10T08_00_01.000000000Z.rcd.gz", 2024, 3, 10, 8, 0, 1));
            writer.writeRecordFile(createTestRecordFile("record0.0.3/2024-03-10T08_00_02.000000000Z.rcd.gz", 2024, 3, 10, 8, 0, 2));
        }

        Path listingFile = ListingRecordFile.getFileForDay(tempDir, year, month, day);
        assertTrue(Files.exists(listingFile), "Listing file should exist");

        // Verify content
        try (DataInputStream in = new DataInputStream(Files.newInputStream(listingFile))) {
            long count = in.readLong();
            assertEquals(3, count, "Should have 3 entries");
        }
    }

    private ListingRecordFile createTestRecordFile(String path, int year, int month, int day, int hour, int minute, int second) {
        return new ListingRecordFile(
                path,
                LocalDateTime.of(year, month, day, hour, minute, second),
                1000, // sizeBytes
                "00112233445566778899aabbccddeeff" // md5Hex (32 chars)
        );
    }
}
