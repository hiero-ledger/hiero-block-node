// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.listing.DayListingFileReader.loadRecordsFileForDay;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.metadata.MetadataFiles;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
@Command(name = "ls-day-listing", description = "Print all files in listing for a day")
public class LsDayListing implements Runnable {

    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored")
    private Path listingDir = MetadataFiles.LISTINGS_DIR;

    @Parameters(index = "0", description = "From year to list")
    private int fromYear = 2019;

    @Parameters(index = "1", description = "From month to list")
    private int fromMonth = 9;

    @Parameters(index = "2", description = "From day to list")
    private int fromDay = 13;

    @Override
    public void run() {
        try {
            lsDayListing(listingDir, fromYear, fromMonth, fromDay);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void lsDayListing(final Path listingDir, final int year, final int month, final int day)
            throws IOException {
        System.out.println("Listing for " + year + "-" + month + "-" + day);

        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir, year, month, day);
        // group by RecordFile.block and process each group
        final Map<LocalDateTime, List<ListingRecordFile>> filesByBlock =
                allDaysFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));
        // print all groups
        for (var entry : filesByBlock.entrySet()) {
            final LocalDateTime blockTime = entry.getKey();
            final List<ListingRecordFile> filesInBlock = entry.getValue();
            System.out.println(" Block time: " + blockTime + " (" + filesInBlock.size() + " files)");
            for (ListingRecordFile file : filesInBlock) {
                System.out.println("  - " + file.path() + " | size: " + file.sizeBytes() + " | md5: " + file.md5Hex());
            }
        }
    }
}
