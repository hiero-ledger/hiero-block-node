// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.listing;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to read the listing file for a specific day and return the list of RecordFile objects.
 */
public class DayListingFileReader {
    /**
     * Load the listing file for the given day and return a list of RecordFile objects.
     * If the file does not exist, return an empty list.
     *
     * @param listingDir the base directory for listings
     * @param year the year
     * @param month the month (1-12)
     * @param day the day (1-31)
     * @return the list of RecordFile objects
     * @throws IOException if an I/O error occurs
     */
    public static List<ListingRecordFile> loadRecordsFileForDay(
            final Path listingDir, final int year, final int month, final int day) throws IOException {
        final Path listingPath = ListingRecordFile.getFileForDay(listingDir, year, month, day);
        return loadRecordsFile(listingPath);
    }

    /**
     * Load the listing file for the given day and return a list of RecordFile objects.
     * If the file does not exist, return an empty list.
     *
     * @param listingPath the path to the listing file
     * @return the list of RecordFile objects
     * @throws IOException if an I/O error occurs
     */
    public static List<ListingRecordFile> loadRecordsFile(Path listingPath) throws IOException {
        final List<ListingRecordFile> recordFiles = new ArrayList<>();
        // Implementation to read the listing file for the given day and return a list of RecordFile objects
        try (var din = new DataInputStream(new BufferedInputStream(Files.newInputStream(listingPath), 1024 * 1024))) {
            final long numberOfFiles = din.readLong();
            for (long i = 0; i < numberOfFiles; i++) {
                recordFiles.add(ListingRecordFile.read(din));
            }
            // double check there are no remaining bytes
            if (din.available() > 0 || din.readAllBytes().length > 0) {
                throw new IOException("Unexpected extra bytes in listing file: " + listingPath);
            }
        }
        return recordFiles;
    }
}
